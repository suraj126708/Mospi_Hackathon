// mospi-api-gateway/routes/data.js (API routes for microdata)

const express = require("express");
const router = express.Router();
const dataController = require("../controllers/dataController");

// GET /api/v1/data/:surveyId/:levelId - Retrieve microdata for a specific survey level
// Supports pagination via 'page' and 'limit' query parameters
// Supports filtering via 'filter' query parameter (JSON string)
router.get("/data/:surveyId/:levelId", dataController.getMicrodata);

// GET /api/v1/data/:surveyId/:levelId/:unitIdentifier - Retrieve a single record by unit_identifier
router.get(
  "/data/:surveyId/:levelId/:unitIdentifier",
  dataController.getMicrodataByUnitIdentifier
);

module.exports = router;
