/*
📓 Data Engineer Skillset Improvements
After completing the project, Kishore might join the Data Engineering team,
as they've shown interest in his work. However, he knows there's more to learn.
Before presenting his pipeline to the DE Team Manager, he requests a code review
from current DE team members.

They review his work, offer congratulations, and suggest improvements:

Add file metadata columns to track loaded files and timestamps.
Integrate PL_LOGS view logic into a single SELECT for simplicity.
Create a new target table if the SELECT logic changes.
Update the COPY INTO statement to use the new SELECT and target table.
Consider an Event-Driven Pipeline instead of a Time-Driven one.
Kishore is eager to implement these suggestions and seeks further guidance.
The team agrees to help, as they are interested in having him handle junior tasks
and eventually move on to more complex work.

Kishore starts on the changes and writes an enhanced SELECT statement.

TIP: Review the SELECT statement carefully. Familiar techniques from Badge 4:
Data Lake Workshop may apply.*/

-- 🥋 A New Select with Metadata and Pre-Load JSON Parsing 


  SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');

-- Create a new table CTAS
/*CTAS creates the table and loads data in one step, but sets VARCHAR fields 
very large. If you use CTAS to define your table, tweak the definition after 
creation.*/

CREATE TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS AS SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');
-- 🥋 Create the New COPY INTO 


--truncate the table rows that were input during the CTAS, if that's what you did
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);



-- If you copy the definition into a worksheet to edit, all rows are wiped out.
-- That's okay since we'll write a COPY INTO next. FYI: We set VARCHARS to 100
-- characters, except USER_EVENT, which is set to 25.

/*
📓 Developing Confidence as a Data Engineer
Kishore is proud of his new COPY INTO code but had to trial and error a lot to 
get it right. 
Here's what he did:

5 searches on docs.snowflake.com
3 visits to community.snowflake.com
2 tips from Stack Overflow
This process involved significant effort and research, but it doesn't mean Kishore 
lacks the skills.
In fact, using documentation and forums is a core part of being a successful 
Data Engineer. 
His persistence and curiosity are key traits of a good Data Engineer.

Key Points:

Data Engineers frequently use documentation and community resources.
Seeking feedback from experienced engineers is essential.
Design decisions often involve trade-offs; balancing maintenance and atomicity is 
part of the job.
Being creative and knowing what tools like Snowflake can do is crucial.


📓 Event-Driven Pipelines
Kishore's old pipeline was Time-Driven. The major alternative is Event-Driven 
Pipelines,
which use Snowpipe. Snowpipe reacts to events, such as new files in a bucket, 
to trigger data processing. 
This method can be more efficient and cost-effective than Time-Driven Pipelines.


📓 Review, Progress, and Next Steps

Let's review our old Task-Driven pipeline, recent changes, and our end goal. We 
created a Task-Driven Pipeline with 4 steps. Recently, Kishore, with help from 
other DEs, created a COPY INTO that removes Step 3 View logic. This change 
means the old Step 2 Task won't be needed and will be replaced by a Snowpipe. 
The COPY INTO Kishore made will be part of the Snowpipe. Step 4, the 
LOAD_LOGS_ENHANCED task, will be edited to point at a new source table and will 
be part of our new Event-Driven Pipeline, which we will complete later.

The image below previews our upcoming Event-Driven Pipeline. You may wonder 
about the HUB, flags, and conveyor belt—these are cloud infrastructure objects 
Snowflake uses for continuous loading. Before creating the Snowpipe, it will 
be helpful for Kishore (and you) to understand how they work.


📓 Cloud-Based Services for Modern Data Pipelines
Key Services:
Modern data pipelines depend on cloud-based services offered by major cloud 
providers like Amazon Web Services (AWS), Microsoft Azure, and Google Cloud 
Platform (GCP).

Creating an Event-Driven Pipeline in Snowflake depends on 3 types of services 
created and managed by the cloud providers.

Storage:

AWS: S3 Buckets
Azure: Blob Storage
GCP: GCS Buckets
Publish & Subscribe Notification Services:

AWS: SNS
Azure: Azure Web PubSub, Azure Event Hub
GCP: Cloud Pub/Sub
Message Queuing:

AWS: SQS
Azure: Azure Storage Queues, Azure Service Bus Queues
GCP: Cloud Tasks
Pub/Sub Services: Operate on a Hub and Spoke model where messages are sent and 
received through a central 
                  HUB and various SPOKES.


Pub/Sub services use a Hub and Spoke pattern. The HUB manages message flow, 
while SPOKES send or receive messages. A SPOKE can be a PUBLISHER (sending) 
or a SUBSCRIBER (receiving), or both.

With many messages in a Pub/Sub service, confusion can arise. EVENT 
NOTIFICATIONS and TOPICS help manage this. A TOPIC groups event types, and 
a SPOKE publishes to or subscribes to a TOPIC, receiving or sending 
NOTIFICATIONS.

🔭 Kishore Logs into His AWS Console Account
No need to log in or sign up for AWS. Just read along!

He creates an SNS topic named `dngw_topic`.

In the bucket, he sets up an Event Notification called 
`a_new_file_is_here`. It triggers when a file is PUT into the bucket, 
sending a notification to `dngw_topic`.

You don't need to perform these steps. A video by QuikStarts can 
show you the process. 

🔭 Kishore Gets an SNS IAM Policy and Adds it to His Topic
Just observe this step; don't run it in your Snowflake trial account.

Kishore receives a policy from Snowflake allowing a service account to 
subscribe to `dngw_topic`.

He adds the policy to `dngw_topic` without replacing the entire policy.

🔭 Kishore Sets Up a Snowpipe
Just observe; you'll perform similar steps later.

Kishore creates a Snowpipe in Snowflake using the `AWS_SNS_TOPIC` 
property. The pipe returns a "Notification Channel Name" key. He finds 
the new subscription endpoint in AWS Console matches this name.

His Snowpipe setup is complete!
*/