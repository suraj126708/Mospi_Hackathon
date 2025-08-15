// mospi-api-gateway/config/db.js (Database connection configuration)

// Export database connection details from environment variables
module.exports = {
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: 5432, // Default PostgreSQL port
};
