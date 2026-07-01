const { Client } = require('pg');
const fs = require('fs');

async function run() {
    const client = new Client({
        user: 'admin',
        password: 'admin',
        host: 'localhost',
        port: process.env.PGPORT || 5433,
        database: 'test_ui.ksql'
    });

    try {
        await client.connect();
        const schema = fs.readFileSync('test-app/schema.sql', 'utf8');
        const queries = schema.split(';').filter(q => q.trim().length > 0);
        for (let q of queries) {
            console.log(`Executing: ${q}`);
            await client.query(q);
        }
        console.log("Schema initialized");
    } catch (err) {
        console.error("Initialization error", err);
    } finally {
        await client.end();
    }
}

run();
