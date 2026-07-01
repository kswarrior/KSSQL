const { Client } = require('pg');

async function debug() {
    const client = new Client({
        user: 'admin',
        password: 'admin',
        host: 'localhost',
        port: process.env.PGPORT || 5433,
        database: 'test_ui.ksql'
    });

    try {
        await client.connect();
        const res = await client.query('SELECT * FROM users');
        console.log("Users:", JSON.stringify(res.rows));
    } catch (err) {
        console.error("Debug error", err);
    } finally {
        await client.end();
    }
}

debug();
