// mospi-api-gateway/routes/data.js (API routes for microdata)

const express = require("express");
const router = express.Router();
const dataController = require("../controllers/dataController");

router.get("/data/:surveyId/:levelId", dataController.getMicrodata);

router.get(
  "/data/:surveyId/:levelId/:unitIdentifier",
  dataController.getMicrodataByUnitIdentifier
);

module.exports = router;
