const express = require('express');
const { Client } = require('pg');
const cookieParser = require('cookie-parser');
const path = require('path');

const app = express();
const port = 4949;

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(cookieParser());
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

const dbConfig = {
    user: 'admin',
    host: 'localhost',
    database: 'ksql',
    password: 'admin',
    port: process.env.PGPORT || 5432,
};

async function query(sql, params) {
    const client = new Client(dbConfig);
    await client.connect();
    try {
        const res = await client.query(sql, params);
        return res;
    } finally {
        await client.end();
    }
}

app.get('/', (req, res) => {
    if (req.cookies.user) {
        res.redirect('/dashboard');
    } else {
        res.redirect('/login');
    }
});

app.get('/login', (req, res) => {
    res.render('login', { error: null });
});

app.post('/login', async (req, res) => {
    const { username, password } = req.body;
    try {
        const result = await query(`SELECT * FROM users WHERE username = '${username}' AND password = '${password}'`);
        if (result.rows.length > 0) {
            res.cookie('user', username);
            res.redirect('/dashboard');
        } else {
            res.render('login', { error: 'Invalid username or password' });
        }
    } catch (err) {
        res.render('login', { error: 'Database error: ' + err.message });
    }
});

app.get('/register', (req, res) => {
    res.render('register', { error: null });
});

app.post('/register', async (req, res) => {
    const { username, password } = req.body;
    try {
        await query(`INSERT INTO users (username, password) VALUES ('${username}', '${password}')`);
        res.redirect('/login');
    } catch (err) {
        res.render('register', { error: 'Registration failed: ' + err.message });
    }
});

app.get('/dashboard', async (req, res) => {
    if (!req.cookies.user) {
        return res.redirect('/login');
    }
    try {
        const result = await query('SELECT * FROM users');
        // Handle table prefixes in result rows
        const processedRows = result.rows.map(row => {
            const newRow = {};
            for (let key in row) {
                const cleanKey = key.includes('.') ? key.split('.')[1] : key;
                newRow[cleanKey] = row[key];
            }
            return newRow;
        });
        res.render('dashboard', { user: req.cookies.user, allUsers: processedRows });
    } catch (err) {
        res.send('Error loading dashboard: ' + err.message);
    }
});

app.get('/logout', (req, res) => {
    res.clearCookie('user');
    res.redirect('/login');
});

app.listen(port, () => {
    console.log(`Test app listening at http://localhost:${port}`);
});
