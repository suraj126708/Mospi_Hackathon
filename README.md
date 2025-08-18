### 🎥 Demo Video and images

-   **Video and images of Demo:** [[Link to a video demonstrating the API in action]](https://drive.google.com/drive/folders/1pCeGmJAZsiBxW6bixIuVurzPKDC35RVU?usp=sharing)

*** 

### Automated Data Dissemination Gateway for MoSPI Microdata

### 💡 Project Overview

This project proposes and details a solution for building an **Automated Data Dissemination Gateway** to modernize how the Ministry of Statistics and Programme Implementation (MoSPI) provides access to its vast collection of microdata. The solution transitions from a manual, file-based system to an intelligent, API-driven platform, aligning with MoSPI's vision for "Statistics-as-a-Service" and India's Digital Public Infrastructure (DPI).

### 🎯 Problem Statement

MoSPI's current system for data dissemination is challenged by a lack of a unified, searchable platform. The data is heterogeneous, with each of the 170+ surveys having a unique structure, requiring manual file downloads and complex processing. This leads to slow "time-to-insight" for researchers and policymakers and creates a barrier to equitable data access. The solution must address these issues while strictly adhering to data privacy and security policies.



### 🚀 Proposed Solution

The core of the solution is an API Gateway that acts as a secure and intelligent interface between users and MoSPI's microdata. The system is built on a metadata-driven architecture to handle all surveys dynamically. 

1.  **Unified Data Architecture:** Ingest over 170 diverse surveys into a single PostgreSQL database. The system uses metadata to dynamically define a flexible schema, handling data heterogeneity without manual intervention for each new survey.

2.  **Automated Ingestion Pipeline:** An automated pipeline will parse metadata, transform raw microdata into a unified JSONB format, and bulk-load it. This eliminates manual effort and ensures scalability as new survey rounds are released.

3.  **Smart, Privacy-Preserving API Layer:** A RESTful API dynamically generates SQL queries based on user requests. It uses metadata to infer data levels and link records across surveys, while programmatically enforcing privacy measures like cell suppression to prevent re-identification.

4.  **Equitable Access with Role-Based Control:** The API provides tiered access for different user roles—from public users to researchers—ensuring everyone can access data at the appropriate level of granularity while maintaining compliance.

5.  **Alignment with Global Standards:** The project modernizes data dissemination and aligns with the UN's Fundamental Principles of Official Statistics, mirroring successful international examples like the Eurostat API.

***

### ⚙️ Core Technical Flow

The project consists of three main components: Automated Ingestion, the Database, and the API Gateway.

#### 1. Automated Ingestion Pipeline
This process is designed to handle the thousands of microdata files and their unique metadata efficiently.



-   **Data Sources:** Raw microdata files (text/CSV) and metadata documents (XLSX, PDF) are the primary inputs.
-   **Metadata Processing:** An automated process extracts and converts metadata into a structured JSON format, which is then stored in a PostgreSQL database in tables like `surveys` and `survey_levels`.
-   **Microdata Processing:** The pipeline uses the stored metadata schemas to dynamically process microdata files, format the data into a JSONB `data_payload`, and bulk-load it into a `survey_data` table.

#### 2. API Gateway & Query Processing

This is the front-facing component that handles all user requests.



-   **User Query:** A user submits a query to the API (e.g., `/api/hces/data?state=MH&item=Cereals`).
-   **API Backend:** The backend dynamically generates a SQL query by referencing the metadata to infer which levels and variables are needed.
-   **Dynamic SQL Execution:** The dynamically built SQL query is executed against the database.
-   **Privacy & Formatting:** The retrieved raw aggregated data is then processed to enforce privacy rules (e.g., cell suppression) and formatted into a user-friendly JSON or CSV output.

***

### 🛠️ Tech Stack

-   **Core Database:** PostgreSQL (with JSONB support)
-   **Backend Language & Framework:** Python (FastAPI / Flask)
-   **Data Processing:** Pandas, custom Python parsers
-   **API Management:** Nginx / Envoy (as API Gateway)
-   **Security:** OAuth2 / API Keys (for authentication)
-   **Deployment:** Docker & Kubernetes
-   **Monitoring:** Prometheus, Grafana
-   **Documentation:** OpenAPI / Swagger

***

### 📚 References & Research

-   **Official MoSPI Guidelines & Principles:**
    -   Draft Revised Guidelines for Statistical Data Dissemination (GSDD): [https://www.mospi.gov.in/sites/default/files/Draft-Revised_GSDD_02012025.pdf](https://www.mospi.gov.in/sites/default/files/Draft-Revised_GSDD_02012025.pdf)
    -   MoSPI Guidelines for National Data Sharing and Accessibility Policy (NDSAP): [https://mospi.gov.in/sites/default/files/announcements/Guidelines_NDSAP.pdf](https://mospi.gov.in/sites/default/files/announcements/Guidelines_NDSAP.pdf)
-   **Foundational Principles of Official Statistics:**
    -   UN Fundamental Principles of Official Statistics: [https://digitallibrary.un.org/record/766579](https://digitallibrary.un.org/record/766579)
-   **Global Examples of Statistical APIs:**
    -   EurostatAPI.jl Documentation (demonstrates programmatic access to European official statistics): [https://ribo.pages.nilu.no/EurostatAPI.jl](https://ribo.pages.nilu.no/EurostatAPI.jl)

***
