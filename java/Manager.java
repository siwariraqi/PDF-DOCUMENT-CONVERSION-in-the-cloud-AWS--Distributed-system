
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Manager {
    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
    public static AmazonEC2 ec2Toolkit = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static AmazonS3 s3Toolkit = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static AmazonSQS sqsToolkit = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    static String newbucket = null;
    public static String queueURL = "https://sqs.us-east-1.amazonaws.com/580358752030/newTask";
    private static S3Object s3object; //contains all the PDF input file
    public static String myQueueUrl = null;
    public static int fileSize = 0;


    public static void main(String args[]) throws IOException, InterruptedException {
        System.out.println("Manager side: --- manager is heree=! ---");
        System.out.println("Manager side: --- handle files per worker ---");
        int numOfFilesPerWorker = -1;
        boolean isDone = false;
        while (!isDone) {
            /** go over all the urls and find the done task queue*/
            for (final String allurls : sqsToolkit.listQueues().getQueueUrls()) {
                final String[] queueName = allurls.split("/");
                if (queueName[queueName.length - 1].contentEquals("filesPerWorker")) {
                    isDone = true;
                    final String specificurl = sqsToolkit.getQueueUrl(new GetQueueUrlRequest("filesPerWorker")).getQueueUrl();
                    final Message message = sqsToolkit.receiveMessage(new ReceiveMessageRequest(specificurl)).getMessages().get(0);
                    numOfFilesPerWorker = Integer.parseInt(message.getBody());
                    System.out.printf("num of workers is here in the manager %d.\n%n", numOfFilesPerWorker);
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            sqsToolkit.deleteQueue(new com.amazonaws.services.sqs.model.DeleteQueueRequest(specificurl));
                        }
                    }).start();
                }
            }
        }

        System.out.print("Manager side: --- Manager downloads list of PDF files together with the operations --- \n");
        /* First get the bucket name that was sent by LocalApplication*/
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueURL);
        final Message message = sqsToolkit.receiveMessage(request).getMessages().get(0);
        newbucket = message.getBody();
        System.out.printf("num of workers is here in the manager %s.\n%n", newbucket);
        try {
            /* get the inputFile from the bucket */
            S3Object object = s3Toolkit.getObject(new GetObjectRequest(newbucket, "inputFile"));
            new Thread(new Runnable() {
                @Override
                public void run() {
                    sqsToolkit.deleteMessage(queueURL, message.getReceiptHandle());
                }
            }).start();
            s3object = object;
        } catch (AmazonS3Exception aex) {
            System.out.println(aex.getErrorMessage());
            s3object = null;
        }

        System.out.print("Manager side: ---  creates an SQS message for each URL and operation from the input list --- \n");

        BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
        myQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest("newPDFTask")).getQueueUrl();
        while (true) {
            String line = reader.readLine();
            if (line == null) break;
            sqsToolkit.sendMessage(new SendMessageRequest(myQueueUrl, line));
            fileSize++;
        }
        System.out.print(String.format("--- the size of the inputFile: [%d] \n", fileSize));

        double muchOfWorkers = Math.ceil(fileSize / numOfFilesPerWorker);
        //todo: delete bucket
        s3Toolkit.deleteObject(new DeleteObjectRequest(newbucket,"inputFile"));

        System.out.print("Manager side: ---  Manager bootstraps workers to process messages --- \n");

        /**Launch the Workers based on the number of the inputFile*/
        DefaultAWSCredentialsProviderChain credentials = new DefaultAWSCredentialsProviderChain();
        AmazonEC2 ec2Client = AmazonEC2ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        TagSpecification tagSpecification = new TagSpecification().withResourceType("instance").withTags(new Tag().withKey("Name").withValue("Worker"));
        /** prepare a script for the ec2 node to run after launching*/
        String[] s = {
                "#!/bin/bash",
                "sudo yum install -y java-1.8.0-openjdk",
                "export AWS_ACCESS_KEY_ID=ASIAYOIATY4PKTKQFHFR",
                "export AWS_SECRET_ACCESS_KEY=F6jS2SSgKeMfRqNXCB6Ou2X8JtVCggFoaziVRCGi",
                "export AWS_SESSION_TOKEN=FwoGZXIvYXdzEHQaDFGj9Cxmq9bjS1cF1CLGAdBKfCdscCHHrFGs0r7690tGlZpKbEona48dgb5iSrIjIZ6HM3LF6iMXFScWMm+36wEXUx2F8Oa6Ev/Iuo+8ITaBam1vcQ2kveXll0Qm9TplFwBYphhWTFcNb2U+p28yOyjmUlIbMTQ0cItYDVgXe89oRuaVCOXMTRchSbyMo+T40jb2T/sOYbOCVuQOvXJRz6ToZpeAFnZ/Us7VrH+TJuglybwfcALgq7LR5XaY5SqceV775/ZXzNbwWvkJ5j5ZSJCIUfkUxSjel96NBjItllDqrhxmgTwHJtB6uKlgLpbPSDFnRcPaHtE1RcsJ69zb13Tfjw/s0m2j2/C1",
                "export AWS_DEFAULT_REGION=us-east-1",
                "aws s3 cp s3://newjarsbucket/Worker.jar Worker.jar",
                "java -jar Worker.jar"
        };
        String script = new String(Base64.encodeBase64(String.join("\n", s).getBytes()), StandardCharsets.UTF_8);
        RunInstancesRequest runRequest = new RunInstancesRequest()
                .withImageId("ami-00e95a9222311e8ed")
                .withMaxCount((int) muchOfWorkers)
                .withMinCount((int) muchOfWorkers)
                .withInstanceType(InstanceType.T2Micro)
                .withTagSpecifications(tagSpecification)
                .withUserData(script);
        ec2Client.runInstances(runRequest);
        System.out.println("Done.\n [" + (int) muchOfWorkers + "]" + " Workers have been launched successfully\n");
        Thread.sleep(30000);

        System.out.println("Manager side: --- Manager reads all Workers' messages from SQS and creates one summary file, once all URLs" +
                "in the input file have been processed. --- ");
        List<Message> sqsMessages = new LinkedList<Message>();
        int size = -1;
        boolean isDonetask = false;
        while (!isDonetask) {
            /** go over all the urls and find the done task queue*/
            for (final String urls : sqsToolkit.listQueues().getQueueUrls()) {
                final String[] queueName = urls.split("/");
                if (queueName[queueName.length - 1].contentEquals("donePDFTask"))
                    isDonetask = true;
            }
        }

        Thread.sleep(4000);
        System.out.println("Manager side: --- Creating summary file --- ");
        createSummaryFile();
        System.out.println("Manager side: --- handle termination --- ");
        Thread.sleep(2000);
        Terminate();

    }

    private static void createSummaryFile() throws IOException, InterruptedException {
        String url = sqsToolkit.getQueueUrl("donePDFTask").getQueueUrl();
        List<Message> messages = new LinkedList<Message>();
        Thread.sleep(3000);
        while (true) {
            List<Message> allmsg = sqsToolkit.receiveMessage(new ReceiveMessageRequest(url)).getMessages();
            if (allmsg != null && allmsg.size()>0) {
                if (allmsg.get(0) != null) {
                    messages.add(allmsg.get(0));
                    sqsToolkit.deleteMessage(new DeleteMessageRequest(url, allmsg.get(0).getReceiptHandle()));
                }
                if (messages.size() == fileSize)
                    break;
            }
        }
        FileWriter fstream;
        BufferedWriter out;
        try {
            fstream = new FileWriter(new File("outputFile.txt"));
            out = new BufferedWriter(fstream);
            for (Message message : messages) {
                out.write(message.getBody() + "\n");
            }
            out.close();
            fstream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //upload it to S3..
        s3Toolkit.putObject(new PutObjectRequest(newbucket, "outputFile.txt", new File("outputFile.txt")));
        final String queueurl = sqsToolkit.createQueue(new CreateQueueRequest("doneTask")).getQueueUrl();
        sqsToolkit.sendMessage(new SendMessageRequest(queueurl, newbucket));
        System.out.println("Done.\n");

    }

    private static void Terminate() {
        System.out.print("Manager side: ---Checking for Terminate Message from LocalApplication ---\n");
        boolean isTerminateHere = false;
        while (!isTerminateHere) {
            for (final String url : sqsToolkit.listQueues().getQueueUrls()) {
                final String[] splits = url.split("/");
                if (splits[splits.length - 1].contentEquals("terminate"))
                    isTerminateHere = true;
            }
        }
        System.out.print("Manager side: --- I got termination message from local ---\n");
        List<Reservation> reservations = ec2Toolkit.describeInstances().getReservations();
        ArrayList<String> instancesIds = new ArrayList<String>();
        for (Reservation res : reservations)
            for (Instance instance : res.getInstances())
                instancesIds.add(instance.getInstanceId());
        TerminateInstancesRequest terminate = new TerminateInstancesRequest();
        terminate.setInstanceIds(instancesIds);
        ec2Toolkit.terminateInstances(terminate);
        sqsToolkit.deleteQueue(new DeleteQueueRequest(sqsToolkit.getQueueUrl(new GetQueueUrlRequest("terminate")).getQueueUrl()));
        sqsToolkit.deleteQueue(new DeleteQueueRequest(sqsToolkit.getQueueUrl(new GetQueueUrlRequest("newTask")).getQueueUrl()));
        sqsToolkit.deleteQueue(new DeleteQueueRequest(sqsToolkit.getQueueUrl(new GetQueueUrlRequest("newPDFTask")).getQueueUrl()));
        sqsToolkit.deleteQueue(new DeleteQueueRequest(sqsToolkit.getQueueUrl(new GetQueueUrlRequest("donePDFTask")).getQueueUrl()));
        System.out.println("IM TERMINATED BY THE LOCAL,AND I TERMINATE MY KIDS -WORKERS-   \n");
    }
}

//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.ec2.AmazonEC2;
//import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
//import com.amazonaws.services.ec2.model.*;
//import com.amazonaws.services.ec2.model.Tag;
//import com.amazonaws.services.ec2.model.TagSpecification;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.*;
//import com.amazonaws.services.sqs.AmazonSQS;
//import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
//import com.amazonaws.services.sqs.model.*;
//import org.apache.commons.codec.binary.Base64;
//
//import java.io.*;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//public class Manager {
//    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
//    private static AmazonEC2 ec2Toolkit;
//    private static AmazonS3 s3Toolkit;
//    private static AmazonSQS sqsToolkit;
//    private static final String bucket = "bucketqOghAwn0EhUw2nJLVYExSMXT5dCZXfwC".toLowerCase();
//    private static final String managerQueueName = "managerQueueqOghAwn0EhUw2nJLVYExSMXT5dCZXfwC"; // Send to Manager
//    static int availableWokers = 0;
//
//    private static String managerQueueUrl;
//    private static String workersQueueName;
//    private static ExecutorService executor;
//    private static boolean terminate = false;
//    private static int numOfTasks = 0;
//    private static String terminatorQueueURL;
//    static List<Message> content = new LinkedList<>();
//
//
//    public static void main(String[] args) {
//
//        System.out.println("Manager side: --- manager is here ---");
//
//        workersQueueName = new Date().getTime() + "workers";
////        executor = Executors.newFixedThreadPool(1);
//
//        ec2Toolkit = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//        s3Toolkit = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//        sqsToolkit = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//        managerQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest(managerQueueName)).getQueueUrl();
//
//
//        System.out.print("Manager side: --- Manager reads Messages --- \n");
//        while (!terminate) {
//            List<Message> messages = sqsToolkit.receiveMessage(new ReceiveMessageRequest(managerQueueUrl)).getMessages();
//            for (Message message : messages) {
//                String[] tokens = message.getBody().split(" ");
//                sqsToolkit.deleteMessage(managerQueueUrl,message.getReceiptHandle());
//                handle_new_task(tokens);
//            }
//        }
////        while (numOfTasks != 0) {
////        }
//        System.out.println("Manager side: --- Terminates all workers, queues, ect ---");
//        Terminate();
//
//        sqsToolkit.deleteQueue(new DeleteQueueRequest(workersQueueName));
//        System.out.println("Manager side: --- After Terminates all workers, queues, ect ---");
//        sqsToolkit.sendMessage("terminate_done", terminatorQueueURL);
//    }
//
//    private static void handle_new_task(String[] tokens) {
//        System.out.println("Manager side: --- Inside handle new task ---");
//        if (tokens[1].equals("terminate")) {
//            System.out.println("Manager side: --- handle termination --- ");
//            terminatorQueueURL = tokens[2];
//            terminate = true;
//            sqsToolkit.deleteQueue(new DeleteQueueRequest(managerQueueUrl));
//        }
//        // new_task | nMsgsPerWorker | bucketKey | localQueue
//        else {
//            System.out.println("Manager side: --- Starting new excector ---");
////            executor.execute(() -> {
//            System.out.println("Starting new excector");
//            numOfTasks++;
//            String senderQueueName = new Date().getTime() + "sender";
//            System.out.println("Manager side: --- initiate queues ---");
//
//            String senderQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest(senderQueueName)).getQueueUrl();
//            String receiverQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest(workersQueueName)).getQueueUrl();
//
//            System.out.println("Manager side: --- Create new Task ---");
//
//            String outFile = tokens[2];
//
//            S3Object object = s3Toolkit.getObject(new GetObjectRequest(bucket, tokens[2]));
//
//
//            // Create message for every line
//            System.out.println("Manager side: --- Create message for every line ---");
//
//            int msgsCounter = 0; // number of messages
//
//            try {
//                BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
//                String line = reader.readLine();
//                while (line != null) {
//                    msgsCounter += 1;
//                    sqsToolkit.sendMessage(new SendMessageRequest(receiverQueueUrl, "new_pdf_task " + line + " " + senderQueueUrl));
//                    line = reader.readLine();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            int numOfWorkers = msgsCounter / Integer.parseInt(tokens[1]);
//
//            List<Reservation> reservations = ec2Toolkit.describeInstances().getReservations();
//            for (Reservation reservation : reservations) {
//                for (Instance instance : reservation.getInstances()) {
//                    boolean isWorker = false;
//                    for (Tag tag : instance.getTags()) {
//                        if (tag.getValue().equals("Worker"))
//                            isWorker = true;
//                    }
//                    if (instance.getState().getName().equals("running") && isWorker)
//                        availableWokers++;
//                }
//            }
//
//            System.out.println("Manager side: --- Creating Workers ---");
//
//            if (numOfWorkers > availableWokers) {
//                DefaultAWSCredentialsProviderChain credentials = new DefaultAWSCredentialsProviderChain();
//                AmazonEC2 ec2Client = AmazonEC2ClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
//                TagSpecification tagSpecification = new TagSpecification().withResourceType("instance").withTags(new Tag().withKey("Name").withValue("Worker"));
//                String[] s = {
//                        "#!/bin/bash",
//                        "sudo yum install -y java-1.8.0-openjdk",
//                        "export AWS_ACCESS_KEY_ID=ASIAYOIATY4PGHFRGC5U",
//                        "export AWS_SECRET_ACCESS_KEY=ZOiGFnCHLk23QTUGFoSJOR4R/y+wI4IA1r0CelbJ",
//                        "export AWS_SESSION_TOKEN=FwoGZXIvYXdzEHAaDBCtTrlurxm3vjB+9iLGAT5iYXWSk7HcIM9xWGV4WgUP2WC1/qiQuLD7lBCBP4zOHdjICzK9N3nbyCRNroNUVm0YZkuHxpudwfUqhhQ8M/6zcpEynh0Ih/9cKN0Xv29olS0+CBKhCZoER9SUwuFyMKhN1PM9Jre07018OZyoyz+01IAYtWfWK82nwVfNYUZK3L8deKCYmOgiqHwmjWPrST9MlSoF69C0ntAJiS6FlrcAU791PrHLh4fxsZpOCVZlzXTSxzc+HKAU/4MOCK7ip6k5pGkBDyj0qt2NBjIt/oFKMTMAU04C/8xBBQOdjvLR+lIvjbYJQyhbZSFi69nQr48CH/9h6yXMKJDY",
//                        "export AWS_DEFAULT_REGION=us-east-1",
//                        "aws s3 cp s3://newjarsbucket/Worker.jar Worker.jar",
//                        "java -jar Worker.jar " + receiverQueueUrl
//                };
//                String script = new String(Base64.encodeBase64(String.join("\n", s).getBytes()), StandardCharsets.UTF_8);
//                RunInstancesRequest runRequest = new RunInstancesRequest()
//                        .withImageId("ami-00e95a9222311e8ed")
//                        .withMaxCount((numOfWorkers - availableWokers))
//                        .withMinCount((numOfWorkers - availableWokers))
//                        .withInstanceType(InstanceType.T2Micro)
//                        .withTagSpecifications(tagSpecification)
//                        .withUserData(script);
//                ec2Client.runInstances(runRequest);
//            }
//            try {
//                Thread.sleep(30000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            // Fetch messages and create pairs of <Image urls, ocr content>
//            String filePath = "summeryFile" + senderQueueName + ".txt";
//            File summeryFile = new File(filePath);
//            while (true) {
//                List<Message> messages = sqsToolkit.receiveMessage(new ReceiveMessageRequest(senderQueueUrl)).getMessages();
//                if (messages != null && messages.size() > 0) {
//                    System.out.println(messages.size() + " ssssssssssssssssssssssssssssssssssssssssssssssss");
//                    System.out.println(msgsCounter + " all msgs  ");
//                    System.out.println(content.size() + " content size");
//                    if (messages.get(0) != null) {
//                        content.add(messages.get(0));
//                        sqsToolkit.deleteMessage(new DeleteMessageRequest(senderQueueUrl, messages.get(0).getReceiptHandle()));
//                    }
//                    if (content.size() == msgsCounter - 1)
//                        break;
//                }
//            }
//
//            // Write the pairs to Summery file
//            System.out.println("Manager side: --- Creating summary file --- ");
//            try {
//                createSummaryFile(content, content.size(), filePath);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//            // Upload summery file to s3 and send done task message to local queue
//            s3Toolkit.putObject(new PutObjectRequest(bucket, filePath, summeryFile));
//            summeryFile.delete();
//            sqsToolkit.sendMessage(new SendMessageRequest(tokens[3], "done_task " + filePath));
////            sqsToolkit.deleteQueue(new DeleteQueueRequest(sqsToolkit.getQueueUrl(new GetQueueUrlRequest(senderQueueUrl)).getQueueUrl()));
////            s3Toolkit.deleteObject(new DeleteObjectRequest(bucket, tokens[1]));
//            numOfTasks--;
////            });
//
//        }
//    }
//
//    private static void createSummaryFile(List<Message> content, int counter, String filePath) throws InterruptedException {
//        try {
//            FileWriter fstream = new FileWriter(new File(filePath));
//            BufferedWriter out = new BufferedWriter(fstream);
//            System.out.println("writing to the file.");
//            for (int i = 0; i < counter; i++)
//                out.write(content.get(i).getBody() + "\n");
//            System.out.println("Successfully wrote to the file.");
//            content.clear();
//            out.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static void Terminate() {
//        System.out.print("Manager side: ---Checking for Terminate Message from LocalApplication ---\n");
//
//        System.out.print("Manager side: --- I got termination message from local ---\n");
//        List<Reservation> reservations = ec2Toolkit.describeInstances().getReservations();
//        ArrayList<String> instancesIds = new ArrayList<>();
//        for (Reservation res : reservations)
//            for (Instance instance : res.getInstances())
//                instancesIds.add(instance.getInstanceId());
//        TerminateInstancesRequest terminate = new TerminateInstancesRequest();
//        terminate.setInstanceIds(instancesIds);
//        ec2Toolkit.terminateInstances(terminate);
//        System.out.println("IM TERMINATED BY THE LOCAL,AND I TERMINATE MY KIDS -WORKERS-   \n");
//    }
//}


