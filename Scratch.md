# GrazeOps Pipeline Guidance

The given project has multiple components, each of which should be handled by different services. The goal of this implementation is to do something mimicking how a real world deployment would look like, while not adding artificial complexity. The final output will be a collection of dockerized services, easily deployable via Docker Compose.

The first task is a simple task, simply creating a container holds the sqlite database and populates it using the provided schema. If time permits, we will switch to PostgreSQL, but it's not a priority. As sqlite is really just a file, this is a bit redundant, but the goal is to mimic a setup with a db as a seperate isolated service. 

The second task is constructing the ingestion pipeline for data, which needs to be a schedulable task and able to accept flexible date ranges. This task is a bit more complicated than it seems at first, as we need to also handle things like the API we are querying also being down, standard issues like malformed requests, etc. Thus the ingestion pipeline needs to support validation and monitoring, ideally also automatically backfilling data for prior periods for which have not been able to collect data due to issues. This should be a service deployable in a single container. The results of each run should be logged as well. The logging of runs ought to be stored in a new table, with options to generate explicit JSON manifests, turned off by default.

The third task is the problem of orchestration for the ingestion pipeline. This task is frankly simple enough that we can run this in the real world with something like an ECS/EKS cron job cleanly implemented in the cloud provider. There are no complex dependencies requiring a more complicated orchestration setup like Apache Airflow. A separate container should be used here to mimic scheduling simply running a periodic cron job. 

The fourth task is the construction of the calculation service, which should store the results of runs in the DB. This too should be a seperate dockerized service. Note that due to the need to demonstrate actual Ops patterns, we should construct an alternate version of the calculation service as well, to be uploaded to a model registry. The calculation service serves the role of mimicking the model in deployments. 

The fifth task is the deployment of a model registry. We should not assume automatically that the common Weights & Biases service/Weave is available for use, though this is often selected in the real world. This model registry should be deployed as another container. We do not need really need to store things like training artefacts here, but we can mention it. 

The sixth task is the automatic staging of builds when a model, or in this case new version of the calculation service is committed. We can handle this through an automatic Docker build process rather than stuff like runners for this simple project. We should have simple tests in this container before the staging of builds. The focus is on SIMPLICITY for tests. CI pipeline will be refined later.

The final task is the creation of a UI to make the whole process easy to examine for reviwers. This UI will be done in a multi page Streamlit app, with dedicated pages for running tests of various services.

Diagrams for this project will be produced by diagrams.py rather than mermaid for more professional rendering. I will manually add icons for the diagrams if time permits. 

Key guardrails:

  - Worker must be idempotent and safe on retries.
  - Add run-lock/lease in DB to prevent overlapping runs.
  - Persist run metadata (run_id, snapshot_id, status, started/ended, error)

Scheduler/Ingestion run metadata

  - ingestion_run_id: unique identifier for a single ingestion run.
  - scheduled_for (TIMESTAMP UTC): when the scheduler intended to run it.
  - started_at (TIMESTAMP UTC): when execution actually started.
  - ended_at (TIMESTAMP UTC): when execution finished.
  - status: run state (running, success, failed).
  - error: failure details if the run failed, otherwise null/empty.
  - snapshot_id: ID of the snapshot produced by this run.

Recommendation/model output metadata

  - recommendation_id: unique identifier for one recommendation record.
  - snapshot_id: snapshot used as input for this recommendation.
  - logic_version: version of calculation/model logic used.
  - config_version: version of parameters/thresholds used.
  - as_of_date (DATE): business date the recommendation is evaluated for.
  - move_date (DATE): recommended herd move date.
  - created_at (TIMESTAMP UTC): when the recommendation record was written.

Do not make code excessively complex. If asked to generate code focus should be on simplicity and readability while fulfilling requirements. The goal is a MVP demonstrating a real world pipeline. 

