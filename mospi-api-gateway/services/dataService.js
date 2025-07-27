// mospi-api-gateway/services/dataService.js (Database query logic for microdata)

const { query } = require("../db");

/**
 * Fetches paginated and filterable microdata for a specific survey level.
 * @param {number} surveyId - The ID of the survey.
 * @param {number} levelId - The ID of the survey level.
 * @param {number} limit - The maximum number of records to return.
 * @param {number} offset - The number of records to skip.
 * @param {Object} filters - An object containing key-value pairs for filtering data_payload.
 * Example: { "Age": { ">": 25 }, "Gender": "Male" }
 * @returns {Promise<{data: Array, totalCount: number}>} - An object containing the data and total count.
 */
exports.findMicrodata = async (surveyId, levelId, limit, offset, filters) => {
  let whereClauses = ["survey_id = $1", "level_id = $2"];
  let params = [surveyId, levelId];
  let paramIndex = 3; // Start index for dynamic filter parameters

  // Build dynamic filter conditions for data_payload
  for (const key in filters) {
    if (filters.hasOwnProperty(key)) {
      const value = filters[key];
      if (typeof value === "object" && value !== null) {
        // Handle operators like >, <, >=, <=, !=
        const operator = Object.keys(value)[0];
        const filterValue = value[operator];
        switch (operator) {
          case ">":
            whereClauses.push(
              `(data_payload->>$${paramIndex})::${
                typeof filterValue === "number" ? "numeric" : "text"
              } > $${paramIndex}`
            );
            break;
          case "<":
            whereClauses.push(
              `(data_payload->>$${paramIndex})::${
                typeof filterValue === "number" ? "numeric" : "text"
              } < $${paramIndex}`
            );
            break;
          case ">=":
            whereClauses.push(
              `(data_payload->>$${paramIndex})::${
                typeof filterValue === "number" ? "numeric" : "text"
              } >= $${paramIndex}`
            );
            break;
          case "<=":
            whereClauses.push(
              `(data_payload->>$${paramIndex})::${
                typeof filterValue === "number" ? "numeric" : "text"
              } <= $${paramIndex}`
            );
            break;
          case "!=":
            whereClauses.push(
              `(data_payload->>$${paramIndex})::${
                typeof filterValue === "number" ? "numeric" : "text"
              } != $${paramIndex}`
            );
            break;
          default:
            // For exact match on object values or other complex JSONB queries,
            // you might use @> operator or custom logic.
            // For simplicity, this example focuses on scalar comparisons.
            whereClauses.push(`data_payload->>$${paramIndex} = $${paramIndex}`);
            break;
        }
        params.push(filterValue);
        paramIndex++;
      } else {
        // Handle exact match for scalar values
        whereClauses.push(`data_payload->>$${paramIndex} = $${paramIndex}`);
        params.push(value);
        paramIndex++;
      }
    }
  }

  const whereClause =
    whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

  // Query for data
  const dataSql = `
        SELECT data_id, unit_identifier, data_payload
        FROM survey_data
        ${whereClause}
        ORDER BY data_id
        LIMIT $${paramIndex} OFFSET $${paramIndex + 1};
    `;
  params.push(limit, offset);

  // Query for total count (without limit/offset)
  const countSql = `
        SELECT COUNT(*) AS total_count
        FROM survey_data
        ${whereClause};
    `;
  // Parameters for count query are the same as data query, excluding limit/offset
  const countParams = params.slice(0, paramIndex - 1);

  const [dataResult, countResult] = await Promise.all([
    query(dataSql, params),
    query(countSql, countParams),
  ]);

  const totalCount = parseInt(countResult.rows[0].total_count);

  return { data: dataResult.rows, totalCount };
};

/**
 * Fetches a single microdata record by its unit_identifier.
 * @param {number} surveyId - The ID of the survey.
 * @param {number} levelId - The ID of the survey level.
 * @param {string} unitIdentifier - The unique identifier for the record.
 * @returns {Promise<Object|null>} - The record object or null if not found.
 */
exports.findMicrodataByUnitIdentifier = async (
  surveyId,
  levelId,
  unitIdentifier
) => {
  const sql = `
        SELECT data_id, unit_identifier, data_payload
        FROM survey_data
        WHERE survey_id = $1 AND level_id = $2 AND unit_identifier = $3;
    `;
  const { rows } = await query(sql, [surveyId, levelId, unitIdentifier]);
  return rows[0] || null;
};
