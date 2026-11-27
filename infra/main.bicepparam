using './main.bicep'

// ---------------------------------------------------------------------------
// Schedule  (all times are UTC)
// ---------------------------------------------------------------------------
// job-bw-data-integration only. Forecast jobs set their own cron in main.bicep.
// 7:00 UTC is 1:00 AM CST, after stores close. 18:00 UTC is noon CST.
// Examples:
//   '0 7,18 * * *'   → 7:00 AM and 6:00 PM UTC (1:00 AM and noon CST)
//   '0 8,20 * * *'   → 8:00 AM and 8:00 PM UTC
//   '0 14,2 * * *'   → 2:00 PM and 2:00 AM UTC
//
// UTC offsets: CST = UTC-6, CDT = UTC-5, EST = UTC-5, EDT = UTC-4
param cronSchedule = '0 7,18 * * *'

// ---------------------------------------------------------------------------
// Image tag
// ---------------------------------------------------------------------------
// This is overridden at deploy time by GitHub Actions with the git commit SHA.
// 'latest' is used for manual deployments (az deployment group create).
param imageTag = 'latest'
