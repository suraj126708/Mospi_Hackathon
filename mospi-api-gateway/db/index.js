// mospi-api-gateway/db/index.js (PostgreSQL client initialization and query helper)

const { Pool } = require("pg");
const dbConfig = require("../config/db");

// Create a new PostgreSQL connection pool
const pool = new Pool(dbConfig);

// Add an error listener to the pool
pool.on("error", (err, client) => {
  console.error("Unexpected error on idle client", err);
  process.exit(-1); // Exit process if a critical database error occurs
});

/**
 * Executes a SQL query using the connection pool.
 * @param {string} text - The SQL query string.
 * @param {Array} params - An array of parameters for the query.
 * @returns {Promise<Object>} - The query result object.
 */
module.exports = {
  query: (text, params) => pool.query(text, params),
};
