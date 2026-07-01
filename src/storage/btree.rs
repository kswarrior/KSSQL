use crate::storage::pager::{Pager, PAGE_SIZE};
use crate::storage::wal::{Wal, WalEntry};
use crate::storage::MemoryTier;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub node_type: NodeType,
    pub keys: Vec<Vec<u8>>,
    pub children: Vec<u64>,
    pub values: Vec<Vec<u8>>,
    pub next_leaf: Option<u64>,
    pub depth: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Record {
    pub value: Vec<u8>,
    pub version: u64,
    pub is_deleted: bool,
    pub timestamp: i64,
}

impl Node {
    pub fn new_leaf(depth: u32) -> Self {
        Node {
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
            depth,
        }
    }

    pub fn new_internal(depth: u32) -> Self {
        Node {
            node_type: NodeType::Internal,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
            depth,
        }
    }

    pub fn to_bytes(&self) -> Result<[u8; PAGE_SIZE]> {
        let encoded = bincode::serialize(self)?;
        if encoded.len() > PAGE_SIZE - 4 {
            return Err(anyhow::anyhow!("Node too large for page size: {} bytes", encoded.len()));
        }
        let mut page = [0u8; PAGE_SIZE];
        // page[0..4] is reserved for checksum in Pager::write_page
        page[4..4 + encoded.len()].copy_from_slice(&encoded);
        Ok(page)
    }

    pub fn from_bytes(bytes: &[u8; PAGE_SIZE]) -> Result<Self> {
        // Pager ensures checksum is valid before we get here
        let node: Node = bincode::deserialize(&bytes[4..])?;
        Ok(node)
    }

    pub fn is_full(&self) -> bool {
        let estimated_size = bincode::serialized_size(self).unwrap_or(0);
        estimated_size > (PAGE_SIZE as u64 * 3 / 4)
    }
}

pub struct BPlusTree {
    pub pager: Arc<Pager>,
    pub wal: Arc<Wal>,
    pub root_page_id: u64,
    pub memory_tier: Arc<MemoryTier>,
}

impl BPlusTree {
    pub async fn open(db_path: &str, wal_path: &str, memory_tier: Arc<MemoryTier>) -> Result<Self> {
        let pager = Arc::new(Pager::open(db_path).await?);
        let wal = Arc::new(Wal::open(wal_path).await?);

        let entries = wal.read_all().await?;
        if !entries.is_empty() {
            for entry in entries {
                match entry {
                    WalEntry::PageUpdate { page_id, data } => {
                        let mut page = [0u8; PAGE_SIZE];
                        let len = data.len().min(PAGE_SIZE);
                        page[..len].copy_from_slice(&data[..len]);
                        pager.write_page(page_id, &page).await?;
                    }
                    WalEntry::RecordUpdate { key, data } => {
                        memory_tier.insert(key, data);
                    }
                    WalEntry::RecordBatch { entries } => {
                        for (key, data) in entries {
                            memory_tier.insert(key, data);
                        }
                    }
                    WalEntry::BinaryBatch { data } => {
                        let mut cursor = 0;
                        while cursor + 4 <= data.len() {
                            let len = u32::from_le_bytes(data[cursor..cursor+4].try_into().unwrap()) as usize;
                            cursor += 4;
                            let tag = u32::from_le_bytes(data[cursor..cursor+4].try_into().unwrap());
                            cursor += 4;
                            if tag == 1 { // RecordUpdate
                                let k_len = u64::from_le_bytes(data[cursor..cursor+8].try_into().unwrap()) as usize;
                                cursor += 8;
                                let key = data[cursor..cursor+k_len].to_vec();
                                cursor += k_len;
                                let v_len = u64::from_le_bytes(data[cursor..cursor+8].try_into().unwrap()) as usize;
                                cursor += 8;
                                let val = data[cursor..cursor+v_len].to_vec();
                                cursor += v_len;
                                memory_tier.insert(key, val);
                            } else {
                                cursor += len - 4;
                            }
                        }
                    }
                    _ => {}
                }
            }
            pager.sync().await?;
        }

        let root_page_id = if pager.num_pages() == 0 {
            let root = Node::new_leaf(0);
            pager.write_page(0, &root.to_bytes()?).await?;
            pager.sync().await?;
            0
        } else {
            0
        };

        Ok(BPlusTree {
            pager,
            wal,
            root_page_id,
            memory_tier,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(val) = self.memory_tier.get(key) {
            return Ok(Some(val));
        }
        let mut current_page_id = self.root_page_id;
        loop {
            let node = self.read_node(current_page_id).await?;
            match node.node_type {
                NodeType::Leaf => {
                    return match node.keys.binary_search(&key.to_vec()) {
                        Ok(idx) => Ok(Some(node.values[idx].clone())),
                        Err(_) => Ok(None),
                    };
                }
                NodeType::Internal => {
                    let mut child_idx = match node.keys.binary_search(&key.to_vec()) {
                        Ok(idx) => idx + 1,
                        Err(idx) => idx,
                    };
                    if child_idx >= node.children.len() {
                        child_idx = node.children.len() - 1;
                    }
                    current_page_id = node.children[child_idx];
                }
            }
        }
    }

    pub async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let root = self.read_node(self.root_page_id).await?;

        if root.is_full() {
            let mut new_root = Node::new_internal(root.depth + 1);
            let old_root_id = self.allocate_page().await?;
            self.save_node(old_root_id, &root).await?;

            new_root.children.push(old_root_id);
            self.split_child(&mut new_root, 0, old_root_id).await?;
            self.save_node(self.root_page_id, &new_root).await?;

            let mut root = self.read_node(self.root_page_id).await?;
            self.insert_non_full(self.root_page_id, &mut root, key, value)
                .await?;
        } else {
            let mut root = root;
            self.insert_non_full(self.root_page_id, &mut root, key, value)
                .await?;
        }
        Ok(())
    }

    async fn insert_non_full(
        &self,
        page_id: u64,
        node: &mut Node,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<()> {
        if node.node_type == NodeType::Leaf {
            match node.keys.binary_search(&key) {
                Ok(idx) => node.values[idx] = value,
                Err(idx) => {
                    node.keys.insert(idx, key);
                    node.values.insert(idx, value);
                }
            }
            self.save_node(page_id, node).await?;
        } else {
            let mut i = match node.keys.binary_search(&key) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            };
            if i >= node.children.len() {
                i = node.children.len() - 1;
            }
            let mut child_id = node.children[i];
            let mut child = self.read_node(child_id).await?;

            if child.is_full() {
                self.split_child(node, i, child_id).await?;
                if key > node.keys[i] {
                    i += 1;
                }
                child_id = node.children[i];
                child = self.read_node(child_id).await?;
            }
            Box::pin(self.insert_non_full(child_id, &mut child, key, value)).await?;
            self.save_node(page_id, node).await?;
        }
        Ok(())
    }

    async fn split_child(&self, parent: &mut Node, i: usize, child_id: u64) -> Result<()> {
        let mut child = self.read_node(child_id).await?;
        let mut new_node = if child.node_type == NodeType::Leaf {
            Node::new_leaf(child.depth)
        } else {
            Node::new_internal(child.depth)
        };

        let mid = child.keys.len() / 2;

        if child.node_type == NodeType::Leaf {
            let mid_key = child.keys[mid].clone();
            new_node.keys = child.keys.split_off(mid);
            new_node.values = child.values.split_off(mid);
            new_node.next_leaf = child.next_leaf;
            let new_node_id = self.allocate_page().await?;
            child.next_leaf = Some(new_node_id);

            parent.keys.insert(i, mid_key);
            parent.children.insert(i + 1, new_node_id);
            self.save_node(new_node_id, &new_node).await?;
        } else {
            let mid_key = child.keys.remove(mid);
            new_node.keys = child.keys.split_off(mid);
            new_node.children = child.children.split_off(mid + 1);
            let new_node_id = self.allocate_page().await?;

            parent.keys.insert(i, mid_key);
            parent.children.insert(i + 1, new_node_id);
            self.save_node(new_node_id, &new_node).await?;
        }

        self.save_node(child_id, &child).await?;
        Ok(())
    }

    async fn allocate_page(&self) -> Result<u64> {
        let id = self.pager.num_pages();
        let blank = [0u8; PAGE_SIZE];
        self.pager.write_page(id, &blank).await?;
        Ok(id)
    }

    pub async fn read_node(&self, page_id: u64) -> Result<Node> {
        let mut cache_key = vec![0u8; 9];
        cache_key[0] = 0xFF; // Non-printable prefix for page namespacing
        cache_key[1..9].copy_from_slice(&page_id.to_le_bytes());

        if let Some(data) = self.memory_tier.get(&cache_key) {
             if let Ok(node) = Node::from_bytes(data.as_slice().try_into().unwrap_or(&[0u8; PAGE_SIZE])) {
                 return Ok(node);
             }
        }
        let bytes = self.pager.read_page(page_id).await?;
        let node = Node::from_bytes(&bytes)?;
        // Populate cache on read
        self.memory_tier.insert_with_priority(cache_key, bytes.to_vec(), node.depth);
        Ok(node)
    }

    pub async fn save_node(&self, page_id: u64, node: &Node) -> Result<()> {
        let bytes = node.to_bytes()?;

        // DEFERRED PERSISTENCE: Write to MemoryTier and mark as dirty
        self.memory_tier.dirty_pages.insert(page_id, bytes.to_vec());

        // Use binary namespaced key to prevent collisions with user records
        let mut cache_key = vec![0u8; 9];
        cache_key[0] = 0xFF; // Non-printable prefix
        cache_key[1..9].copy_from_slice(&page_id.to_le_bytes());

        self.memory_tier.insert_with_priority(cache_key, bytes.to_vec(), node.depth);

        Ok(())
    }
}
