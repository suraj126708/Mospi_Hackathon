// mospi-api-gateway/app.js (Main application file)

// Load environment variables from .env file
require("dotenv").config();

// Import Express and other necessary modules
const express = require("express");
const app = express();
const PORT = process.env.PORT || 3000;

// Import database configuration and client
const { query } = require("./db");

// Import API routes
const surveyRoutes = require("./routes/surveys");
const dataRoutes = require("./routes/data");

// Middleware: Enable JSON body parsing for POST/PUT requests
app.use(express.json());

// --- Basic API Endpoints ---

// 1. Root Endpoint
app.get("/", (req, res) => {
  res.status(200).send("Welcome to the MoSPI Microdata API Gateway!");
});

// Use API routes
// All routes defined in surveyRoutes will be prefixed with /api/v1
app.use("/api/v1", surveyRoutes);
// All routes defined in dataRoutes will be prefixed with /api/v1
app.use("/api/v1", dataRoutes);

// --- Global Error Handling Middleware ---
// This middleware catches any errors thrown by route handlers or other middleware.
app.use((err, req, res, next) => {
  console.error("API Error:", err.stack); // Log the error stack for debugging
  res.status(err.statusCode || 500).json({
    message: err.message || "An unexpected error occurred.",
    error: process.env.NODE_ENV === "production" ? {} : err.message, // Don't expose stack in production
  });
});

// Start the server
app.listen(PORT, () => {
  console.log(`MoSPI API Gateway running on http://localhost:${PORT}`);
  console.log(`API Endpoints:`);
  console.log(` - http://localhost:${PORT}/api/v1/surveys`);
  console.log(` - http://localhost:${PORT}/api/v1/surveys/:surveyId/levels`);
  console.log(
    ` - http://localhost:${PORT}/api/v1/data/:surveyId/:levelId?page=1&limit=10&filter={"Age":{">":25}}`
  );
});
