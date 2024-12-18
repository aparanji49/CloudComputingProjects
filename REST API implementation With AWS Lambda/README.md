# REST API Implementation using AWS Lambda

## Overview

The project involves developing a set of REST API endpoints using AWS Lambda to support a database-backed web application. The APIs enable CRUD operations for books and categories, stored in an RDS database. The project leverages Amazon API Gateway to expose the endpoints and AWS Lambda for serverless function execution. IAM policies were configured to ensure secure access to the AWS resources.

## Tools and Technologies Used

**AWS Lambda:** For executing serverless functions.
**Amazon API Gateway:** To expose and manage the APIs.
**Amazon RDS (MySQL):** As the database backend for storing books and categories.
**IAM:** For access management and secure authentication.
**Gson and JSON Libraries:** For data handling and parsing.
**Java JDBC:** For database interactions.

## Architecture diagram

![image](https://github.com/user-attachments/assets/e715d9f7-2619-4ca5-aa0a-451d4dd7640f)

## Steps Taken

1. Database Setup:
- Configured an Amazon RDS instance for MySQL with a custom database and tables for Book and Category.
- Utilized the free-tier configuration for RDS, ensuring optimal performance within constraints.
2. Development of DAO Classes:
- CategoryDao and BookDao interfaces with methods for CRUD operations.
- Implemented using JDBC in CategoryDaoJdbc and BookDaoJdbc classes to interact with the database securely using environment variables.
3. Lambda Function Implementation:
- Created API endpoints as Lambda functions. Examples include:
getAllCategory, getCategoryName, getAllBook, getBookById.
- Functions like addBook and addCategory perform insert operations.
- Used dbLambdaHandler as the entry point to route API Gateway requests to appropriate database operations.
4. API Gateway Configuration:
- Defined routes and methods (GET, POST) for each API endpoint.
- Configured CORS settings for cross-origin support.
5. IAM Policies:
- Faced challenges with IAM Database Authentication, as it was unavailable in the initially assigned region (Spain). After consulting with the professor, resources were created in the Northern Virginia region.
- Created IAM roles and policies to grant secure access between Lambda functions and RDS, without embedding credentials in code.
6. Testing and Deployment:
- Tested functions locally using mock inputs.
- Deployed to AWS and validated functionality using the API Gateway.
7. Logging:
- Implemented logging within Lambda functions to capture activity and debug information.

## Challenges
- IAM Database Authentication:
The IAM database authentication feature was not visible in the initially assigned region (Spain). After identifying this issue, resources were recreated in the Northern Virginia region, which resolved the problem. This challenge highlighted the importance of region-specific feature availability in AWS services.

## What I Learned
**Serverless Architecture:** Implemented a fully serverless backend leveraging AWS Lambda and API Gateway.
**IAM and Security:** Gained a deeper understanding of IAM roles and policies to secure interactions between AWS services.
**AWS Region-Specific Features:** Learned about the nuances of feature availability in different AWS regions.
**JDBC with MySQL:** Strengthened understanding of using JDBC for database operations.
**API Development:** Built robust and scalable APIs for CRUD operations.


