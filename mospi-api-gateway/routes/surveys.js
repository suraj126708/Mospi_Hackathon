// mospi-api-gateway/routes/surveys.js (API routes for surveys and levels)

const express = require("express");
const router = express.Router();
const surveyController = require("../controllers/surveyController");

// GET /api/v1/surveys - List all available surveys
router.get("/surveys", surveyController.getSurveys);

// GET /api/v1/surveys/:surveyId/levels - List levels for a specific survey
router.get("/surveys/:surveyId/levels", surveyController.getSurveyLevels);

module.exports = router;
