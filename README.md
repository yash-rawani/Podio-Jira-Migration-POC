# Podio to Jira Bug Migration

This project migrates Podio Bugs tickets into Jira Bug issues.

## What the migration does

- Authenticates to Podio and Jira
- Scans Podio Bugs tickets page by page
- Filters tickets by:
  - `VPC` field
  - attachment presence
- Creates Jira Bug issues
- Maps Podio fields to Jira fields:
  - Podio **Created On** -> Jira **Created Date**
  - Podio **Created By** -> Jira **Reporter**
  - Podio **Developer assigned** -> Jira **Developer**
- Uploads Podio file attachments
- Adds Podio comments into Jira comments
- Uploads Podio activity history as a `.txt` attachment
