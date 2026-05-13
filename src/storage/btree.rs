use crate::storage::pager::{Pager, PAGE_SIZE};
use crate::storage::wal::Wal;
use anyhow::Result;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum NodeType {
    Internal,
    Leaf,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Node {
    pub node_type: NodeType,
    pub keys: Vec<Vec<u8>>,
    pub children: Vec<u32>,   // Page IDs for Internal nodes
    pub values: Vec<Vec<u8>>, // Values for Leaf nodes
    pub next_leaf: Option<u32>,
}

impl Node {
    pub fn new_leaf() -> Self {
        Node {
            node_type: NodeType::Leaf,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        }
    }

    pub fn new_internal() -> Self {
        Node {
            node_type: NodeType::Internal,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        }
    }

    pub fn to_bytes(&self) -> Result<[u8; PAGE_SIZE]> {
        let encoded = bincode::serialize(self)?;
        if encoded.len() > PAGE_SIZE {
            return Err(anyhow::anyhow!("Node too large for page size"));
        }
        let mut page = [0u8; PAGE_SIZE];
        page[..encoded.len()].copy_from_slice(&encoded);
        Ok(page)
    }

    pub fn from_bytes(bytes: &[u8; PAGE_SIZE]) -> Result<Self> {
        let node: Node = bincode::deserialize(bytes)?;
        Ok(node)
    }

    pub fn is_full(&self) -> bool {
        // Rough estimate for capacity.
        // In a real DB, this would be based on actual byte size.
        // For simplicity, we use a fixed number of keys.
        self.keys.len() >= 100
    }
}

pub struct BPlusTree {
    pub pager: Pager,
    pub wal: Wal,
    pub root_page_id: u32,
}

impl BPlusTree {
    pub fn open(db_path: &str, wal_path: &str) -> Result<Self> {
        let mut pager = Pager::open(db_path)?;
        let wal = Wal::open(wal_path)?;

        let root_page_id = if pager.num_pages() == 0 {
            let root = Node::new_leaf();
            pager.write_page(0, &root.to_bytes()?)?;
            0
        } else {
            0
        };

        Ok(BPlusTree {
            pager,
            wal,
            root_page_id,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut current_page_id = self.root_page_id;
        loop {
            let node = Node::from_bytes(&self.pager.read_page(current_page_id)?)?;
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

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let root_bytes = self.pager.read_page(self.root_page_id)?;
        let mut root = Node::from_bytes(&root_bytes)?;

        if root.is_full() {
            let mut new_root = Node::new_internal();
            let old_root_id = self.allocate_page()?;
            self.save_node(old_root_id, &root)?;

            new_root.children.push(old_root_id);
            self.split_child(&mut new_root, 0, old_root_id)?;
            self.root_page_id = 0; // Root is always 0 in this simplified impl
            self.insert_non_full(0, &mut new_root, key, value)?;
        } else {
            self.insert_non_full(self.root_page_id, &mut root, key, value)?;
        }
        Ok(())
    }

    fn insert_non_full(&mut self, page_id: u32, node: &mut Node, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if node.node_type == NodeType::Leaf {
            match node.keys.binary_search(&key) {
                Ok(idx) => node.values[idx] = value,
                Err(idx) => {
                    node.keys.insert(idx, key);
                    node.values.insert(idx, value);
                }
            }
            self.save_node(page_id, node)?;
        } else {
            let mut i = match node.keys.binary_search(&key) {
                Ok(idx) => idx + 1,
                Err(idx) => idx,
            };
            let mut child_id = node.children[i];
            let mut child = Node::from_bytes(&self.pager.read_page(child_id)?)?;

            if child.is_full() {
                self.split_child(node, i, child_id)?;
                if key > node.keys[i] {
                    i += 1;
                }
                child_id = node.children[i];
                child = Node::from_bytes(&self.pager.read_page(child_id)?)?;
            }
            self.insert_non_full(child_id, &mut child, key, value)?;
            self.save_node(page_id, node)?;
        }
        Ok(())
    }

    fn split_child(&mut self, parent: &mut Node, i: usize, child_id: u32) -> Result<()> {
        let mut child = Node::from_bytes(&self.pager.read_page(child_id)?)?;
        let mut new_node = if child.node_type == NodeType::Leaf {
            Node::new_leaf()
        } else {
            Node::new_internal()
        };

        let mid = child.keys.len() / 2;
        let mid_key = child.keys.remove(mid);

        new_node.keys = child.keys.split_off(mid);
        if child.node_type == NodeType::Leaf {
            new_node.values = child.values.split_off(mid);
            new_node.next_leaf = child.next_leaf;
            let new_node_id = self.allocate_page()?;
            child.next_leaf = Some(new_node_id);

            parent.keys.insert(i, mid_key);
            parent.children.insert(i + 1, new_node_id);
            self.save_node(new_node_id, &new_node)?;
        } else {
            new_node.children = child.children.split_off(mid + 1);
            let new_node_id = self.allocate_page()?;

            parent.keys.insert(i, mid_key);
            parent.children.insert(i + 1, new_node_id);
            self.save_node(new_node_id, &new_node)?;
        }

        self.save_node(child_id, &child)?;
        Ok(())
    }

    fn allocate_page(&mut self) -> Result<u32> {
        let id = self.pager.num_pages();
        let blank = [0u8; PAGE_SIZE];
        self.pager.write_page(id, &blank)?;
        Ok(id)
    }

    fn save_node(&mut self, page_id: u32, node: &Node) -> Result<()> {
        let bytes = node.to_bytes()?;
        self.wal.log_page_update(page_id, &bytes)?;
        self.pager.write_page(page_id, &bytes)?;
        Ok(())
    }
}
