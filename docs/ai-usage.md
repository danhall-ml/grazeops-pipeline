## Usage of AI

I used AI heavily throughout this project, primarily through the Codex CLI. This was a time-boxed assignment, and AI was useful for speeding up scaffolding, iteration, and documentation. I used it as a tool to accelerate implementation, not as a substitute for architecture judgment.

The internal planning document in `docs/working-notes.md` is the guidance document I provided to Codex at the beginning of the project. It captures the starting service breakdown, priorities, and simplification guardrails that I wanted the implementation to follow.

My usage was roughly as follows:

1. I started by providing an initial design draft that described the desired services, the role of each service, preferred styling, and some constraints each service should satisfy. The work was then broken into smaller tasks.
2. I used Codex to iterate through those tasks and scaffold the services while maintaining a shared `docker-compose` deployment for the stack.
3. I built the containers and checked the deployed stack for issues.
4. I then made another pass through each task and asked what could be simplified. I also checked which responsibilities were better delegated to other services for cleaner isolation.
5. Some of the heavier rework happened in areas like:
   - pushing config-handling responsibility from the scheduler into the ingestion engine,
   - reducing the fake model registry to a much smaller GET/POST style service that was more appropriate for the assignment,
   - trimming unnecessary fields and simplifying stored records.
6. After that, I manually reviewed the code again following the simplification pass.
7. I then did another pass focused on rubric alignment and test generation. During that process, I ran into issues around concurrency and lock behavior, which led me to move from SQLite to PostgreSQL.
8. I also used AI to generate docstrings and some comments in NumPy style where that helped clean up the codebase.

Areas where I was most manually involved were:

1. The Streamlit application, because the initial AI-generated UI quality was not acceptable.
2. Reducing command-line argument complexity across services.
3. Simplifying the `docker-compose` setup for the containerized stack.
4. Simplifying the staging service, because the generated version was more complicated than I thought the assignment justified.
5. Reworking service boundaries when the generated structure introduced unnecessary complexity.

I also used the OpenAI ChatGPT app in a small way for generating icons for diagrams.

Overall, AI was most useful for scaffolding, iteration, and speeding up repetitive work. The parts that required the most manual judgment were service boundaries, simplification, operational design choices, UI quality, and deciding what level of complexity was actually appropriate for this assignment.
