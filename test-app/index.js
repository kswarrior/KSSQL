const express = require('express');
const { Client } = require('pg');
const cookieParser = require('cookie-parser');
const path = require('path');
const crypto = require('crypto');

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

// Security: Simple escaping for single quotes to prevent basic SQL injection
function sanitize(str) {
    if (typeof str !== 'string') return '';
    return str.replace(/'/g, "''");
}

// Security: Secure password hashing with salt
function hashPassword(password, salt) {
    return crypto.createHmac('sha256', salt).update(password).digest('hex');
}

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
    const username = sanitize(req.body.username);
    const password = req.body.password;

    try {
        // Fetch user by sanitized username
        const result = await query(`SELECT * FROM users WHERE username = '${username}'`);
        if (result.rows.length > 0) {
            const user = result.rows[0];
            // Recover row with potential prefixes
            const userData = {};
            for (let k in user) {
                const cleanK = k.includes('.') ? k.split('.')[1] : k;
                userData[cleanK] = user[k];
            }

            // Verify password against stored hash (salt = username for simplicity in this test app)
            const expectedHash = hashPassword(password, userData.username);
            if (userData.password === expectedHash) {
                res.cookie('user', userData.username);
                res.redirect('/dashboard');
                return;
            }
        }
        res.render('login', { error: 'Invalid username or password' });
    } catch (err) {
        res.render('login', { error: 'Database error: ' + err.message });
    }
});

app.get('/register', (req, res) => {
    res.render('register', { error: null });
});

app.post('/register', async (req, res) => {
    const username = sanitize(req.body.username);
    const password = req.body.password;

    if (!username || !password) {
        return res.render('register', { error: 'Username and password required' });
    }

    try {
        const hashed = hashPassword(password, username);
        await query(`INSERT INTO users (username, password) VALUES ('${username}', '${hashed}')`);
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
