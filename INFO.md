<h1>How our program works</h1>
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

our project has 3 main Entities: The Manager, the Worker and the Local Application.
we used the ami-b66ed3de t2.small for out worker instances, and t2.micro for the managaer instance.


We used 5 SQS Queues:
---------------------

(1) tasks - each item in the queue represents a new task added by a local app which will
    be handled by the Manager.
(2) finished tasks - consists of all of the completed tasks which results are ready for 
    the local app to recieve.
(3) raw_tweets - each tweet message from any local app is split into 2 small tasks - 
    entities and sentiment analysis.
(4) processed_tweets - consists of 2 output tweets for each input tweet, one for it's 
    sentiment analysis, and the other for it's entities.
(5) log - the manager, worker and the local-app, are updating this queue in case of errors and exceptions

We also used 4 S3 buckets:
--------------------------

(1) jars - containing all of the jar files that will be downloaded and run by the EC2 instances.
(2) input - containing all the input files uploaded by the local applications.
(3) output - containing all the output files (2 for each local app) uploaded by the manager.
(4) log - contains the logging files of each local application and manager.



	The Local Application
	---------------------

Our local application first upload it's input file to the input bucket.
Then, it sends a Task message instance (serialized) to the tasks queue, and initiates the Manager
EC2 instance (if there is no other Manager instance currently running).
Next, it waits for a Task message from the finished tasks queue, notifying its task (identified
by its unique id) is completed and waiting to be downloaded.
The local app then deletes this message, and (if requested) sends a termination task message to
the tasks queue.
The local app downloads the 2 output files containing it's tweets analysis using 2 InputStream
instances, and creates 2 html files with the results received and colored as requested.



	The Manager
	------------
The Manager maintains a HashMap of <id, taskStatus>, where the task status hold the current total
count for sentiments and entities that were analyzed for the task's tweet.
The Manager constantly handles both receiving new tasks from the tasks queue, and joining processed tweets
to a completed task.
The Manager main loop alternates between the above two actions:
	(1) The Manager checks if there are new tasks (not a terminate task). If so:
				(1) takes the DE serialized message (as a TaskMessage)
				(2) downloads it's relevant input file.
				(3) adds it's tweet to the raw_tweets queue.
				(4) creates workers instances of the amount needed.
				(5) delete task from tasks queue.
	(2) second, the Manager takes the available processed tweets from their queue. and:
				(1) takes the DE serialized message (as a TweetMas).
				(2) writes the tweet's entities or sentiment to the relevant output file.
				(3) update the task's status to the current entities or sentiment counts.
				(4) deletes the processed tweet from the queues.
				(5) If the tweet's task is finished, upload both output files to the
				    output bucket in S3, and adds the tasks to the finished tasks queue.
(*) if the tasks is a termination task, the Manager will terminate all workers only after finishing the tasks
    that are still in process, and will then terminate its self.


	The Worker
	------------
The Worker performs an infinite loop, in which it:
	(1) receives a raw tweet as a message from the raw_tweets queue.
	(2) processes the the tweet according to the TweetMsg's required task (entities or sentiment)
	(3) serializes the TweetMsg and sends it to the processed_tweets queue.
	(4) deletes the TweetMsg from queue.

(*)The worker will be terminated only by the Manager, which will do so only after the worker 
   will complete it's current task.



	Message Classes
	---------------

(1) TweetMsg - each tweet is represented by a AWS Message where the body is the tweet string, attributes of output filename and uuid, task (sentiment or entities)
(2) TaskMessage -  represents a whole task requested by a local app.
(3) TaskStatus - represents the analysis progression status of one task requested by a local app.


	Run Time for input example
	--------------------------
we ran our code on the given input file with n = 30 and it finished in 4:48 minutes (if the manager and
workers are already running - 1:30 minutes).

in order to run app write the following in console (from the folder containing local.jar) :
	
	 java -jar local.jar inputFileName outputFileName n terminate
