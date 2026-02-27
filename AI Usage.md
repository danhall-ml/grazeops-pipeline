## Usage of AI

I use AI heavily throughout the construction of this project to satisfy time constraints. My interactions are primarily through the Codex CLI. My usage is as follows:

1. I create an initial design draft specifying the desired services and a general overview of what the role is of each service. I provide basic comments on preferred styling as well some constraints that each service should satisfy. The construction of each service is broken down into a collection of tasks.
2. I ask Codex to iterate through each of the tasks to construct each service while maintaing a shared docker-compose for deployment of the stack.
3. I build each container and check the docker-compose deployment for any issues.
4. I do another pass through each task, asking for what in each service can be simplified. In addition, I check for which responsibilities should more appropriately be delegated to another service for better isolation. Areas of heavy rework were
  - The scheduler needs to pass off the responsibility of handling the config layer to the ingestion engine.
  - Our fake model registry service honestly only needs to handle GET/POST type behavior for this assignment. We can mention storing model artifacts but that responsibility is excessive for this assignment. The stored fields are also simplfied to a more minimal record. 
6. I manually check through the code after simplification. 
7. I do another pass, checking for alignment against the rubric and also generating a test suite. Unfortunately, I noticed issues after generating the test suite and had to move to PostgreSQL. 
8. I ask for docstrings and comments to be generated according to numpy styling conventions. 

Areas which I am heavily manually involved in are

1. The streamlit application due to low quality AI styling.
2. Removing command line args for each service due to excessive AI preference for complexity
3. Simplifying the docker-compose file for deployment of containers
4. Simplfiying our staging service, as the level of complexity automatically generated I do not think is appropriate for this assignment.

Finally, I also use the OpenAI ChatGPT app for generating icons for diagrams.

## Shared Resps