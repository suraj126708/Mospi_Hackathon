// mospi-api-gateway/controllers/dataController.js (Logic for microdata retrieval endpoints)

const dataService = require("../services/dataService");

/**
 * Controller to handle GET /api/v1/data/:surveyId/:levelId request.
 * Retrieves paginated and filterable microdata for a specific survey level.
 */
exports.getMicrodata = async (req, res, next) => {
  try {
    const surveyId = parseInt(req.params.surveyId);
    const levelId = parseInt(req.params.levelId);

    if (isNaN(surveyId) || isNaN(levelId)) {
      return res
        .status(400)
        .json({ message: "Invalid Survey ID or Level ID provided." });
    }

    // Pagination parameters
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 100; // Default limit
    const offset = (page - 1) * limit;

    // Filtering parameters (assuming JSON string in query param)
    let filters = {};
    if (req.query.filter) {
      try {
        filters = JSON.parse(req.query.filter);
      } catch (jsonError) {
        return res
          .status(400)
          .json({ message: "Invalid JSON format for filter parameter." });
      }
    }

    const { data, totalCount } = await dataService.findMicrodata(
      surveyId,
      levelId,
      limit,
      offset,
      filters
    );

    if (data.length === 0) {
      return res
        .status(404)
        .json({
          message:
            "No data found for the specified survey and level with current filters.",
        });
    }

    res.status(200).json({
      page,
      limit,
      totalCount,
      totalPages: Math.ceil(totalCount / limit),
      data,
    });
  } catch (error) {
    next(error);
  }
};

/**
 * Controller to handle GET /api/v1/data/:surveyId/:levelId/:unitIdentifier request.
 * Retrieves a single microdata record by its unit_identifier.
 */
exports.getMicrodataByUnitIdentifier = async (req, res, next) => {
  try {
    const surveyId = parseInt(req.params.surveyId);
    const levelId = parseInt(req.params.levelId);
    const unitIdentifier = req.params.unitIdentifier;

    if (isNaN(surveyId) || isNaN(levelId) || !unitIdentifier) {
      return res.status(400).json({ message: "Invalid parameters provided." });
    }

    const record = await dataService.findMicrodataByUnitIdentifier(
      surveyId,
      levelId,
      unitIdentifier
    );

    if (!record) {
      return res
        .status(404)
        .json({
          message: "Record not found for the specified unit identifier.",
        });
    }

    res.status(200).json(record);
  } catch (error) {
    next(error);
  }
};
