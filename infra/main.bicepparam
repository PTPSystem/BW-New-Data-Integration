using './main.bicep'

// ---------------------------------------------------------------------------
// Schedule  (all times are UTC)
// ---------------------------------------------------------------------------
// Adjust these to match when you want the job to run.
// Examples:
//   '0 6,18 * * *'   → 6:00 AM and 6:00 PM UTC (default)
//   '0 8,20 * * *'   → 8:00 AM and 8:00 PM UTC
//   '0 14,2 * * *'   → 2:00 PM and 2:00 AM UTC
//
// UTC offsets: CST = UTC-6, CDT = UTC-5, EST = UTC-5, EDT = UTC-4
param cronSchedule = '0 6,18 * * *'

// ---------------------------------------------------------------------------
// Image tag
// ---------------------------------------------------------------------------
// This is overridden at deploy time by GitHub Actions with the git commit SHA.
// 'latest' is used for manual deployments (az deployment group create).
param imageTag = 'latest'
