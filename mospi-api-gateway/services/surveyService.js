// mospi-api-gateway/services/surveyService.js (Database query logic for surveys and levels)

const { query } = require("../db");

/**
 * Fetches all surveys from the 'surveys' table.
 * @returns {Promise<Array>} - An array of survey objects.
 */
exports.findAllSurveys = async () => {
  const sql = `SELECT survey_id, survey_name, survey_year, description FROM surveys ORDER BY survey_year DESC, survey_name ASC;`;
  const { rows } = await query(sql);
  return rows;
};

/**
 * Fetches all levels for a given surveyId from the 'survey_levels' table.
 * @param {number} surveyId - The ID of the survey.
 * @returns {Promise<Array>} - An array of level objects.
 */
exports.findLevelsBySurveyId = async (surveyId) => {
  const sql = `SELECT level_id, level_name, variable_schema, common_identifiers FROM survey_levels WHERE survey_id = $1 ORDER BY level_name ASC;`;
  const { rows } = await query(sql, [surveyId]);
  return rows;
};
