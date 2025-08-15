// mospi-api-gateway/controllers/surveyController.js (Logic for survey and level endpoints)

const surveyService = require("../services/surveyService");

/**
 * Controller to handle GET /api/v1/surveys request.
 * Retrieves all surveys from the database.
 */
exports.getSurveys = async (req, res, next) => {
  try {
    const surveys = await surveyService.findAllSurveys();
    res.status(200).json(surveys);
  } catch (error) {
    // Pass error to global error handling middleware
    next(error);
  }
};

/**
 * Controller to handle GET /api/v1/surveys/:surveyId/levels request.
 * Retrieves all levels for a specific survey from the database.
 */
exports.getSurveyLevels = async (req, res, next) => {
  try {
    const surveyId = parseInt(req.params.surveyId);
    if (isNaN(surveyId)) {
      return res.status(400).json({ message: "Invalid Survey ID provided." });
    }
    const levels = await surveyService.findLevelsBySurveyId(surveyId);
    if (levels.length === 0) {
      return res
        .status(404)
        .json({ message: "Survey not found or no levels available." });
    }
    res.status(200).json(levels);
  } catch (error) {
    next(error);
  }
};
