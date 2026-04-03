const axios = require("axios");
const { createObjectCsvWriter } = require("csv-writer");
const fs = require("fs");

const API_KEY = ""; // get from census.gov

/* ─────────────────────────────────────────────
   LOAD BUSINESS DATA (CBP FORMAT)
───────────────────────────────────────────── */

const businessRaw = JSON.parse(fs.readFileSync("business.json", "utf-8"));

const businessMap = {};

// Skip header row and build FIPS → business_count map
businessRaw.slice(1).forEach((row) => {
  const count = Number(row[0]);
  const state = row[1];
  const county = row[2];

  const fips = state + county;

  businessMap[fips] = count;
});

/* ─────────────────────────────────────────────
   FETCH ACS DATA
───────────────────────────────────────────── */

async function fetchACS() {
  const url = `https://api.census.gov/data/2022/acs/acs5?get=B01003_001E,B19013_001E&for=county:*&in=state:*&key=${API_KEY}`;

  const res = await axios.get(url);
  const rows = res.data;

  const data = rows.slice(1);

  const VALID_STATE_FIPS = new Set([
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
  ]);

  const seen = new Set();

  const result = data
    .filter((row) => VALID_STATE_FIPS.has(row[2]))
    .map((row) => {
      const population = Number(row[0]);
      const median_income = Number(row[1]);
      const state = row[2];
      const county = row[3];

      const fips = `${state}${county}`;

      return {
        fips,
        population,
        median_income,
      };
    })
    // remove duplicates (just in case)
    .filter((row) => {
      if (seen.has(row.fips)) return false;
      seen.add(row.fips);
      return true;
    })
    // merge business data
    .map((row) => ({
      ...row,
      business_count: businessMap[row.fips] ?? 0,
    }));

  return result;
}

/* ─────────────────────────────────────────────
   RUN + EXPORT CSV
───────────────────────────────────────────── */

async function run() {
  const data = await fetchACS();

  console.log("Total counties:", data.length);

  const missingBiz = data.filter((d) => d.business_count === 0);
  console.log("Missing business data:", missingBiz.length);

  const csvWriter = createObjectCsvWriter({
    path: "counties.csv",
    header: [
      { id: "fips", title: "fips" },
      { id: "population", title: "population" },
      { id: "median_income", title: "median_income" },
      { id: "business_count", title: "business_count" },
    ],
  });

  await csvWriter.writeRecords(data);

  console.log("✅ counties.csv generated");
}

run().catch(console.error);
