
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.resources.S3ObjectResource;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.nio.charset.StandardCharsets;

import java.util.List;


public class LocalApplication {
    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
    public static AmazonEC2 ec2Toolkit = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static AmazonS3 s3Toolkit = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static AmazonSQS sqsToolkit = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static String inputFile, outputFile;
    static boolean isTerminate = false;
    public static int numOfFilesPerWorker;
    static String handleQueueUrl = null;
    public static String newbucket;
    public static String key = null;

    public static void main(String[] args) throws InterruptedException, IOException {
        inputFile = args[0];
        outputFile = args[1];
        numOfFilesPerWorker = Integer.parseInt(args[2]);
        if (args[3].equals("terminate"))
            isTerminate = true;

        System.out.println("Local side: --- initial LocalApp ---");

        System.out.println("Local side: --- update the manager of the num of the workers that's needed ---");
        try {
            final String queueUrl = sqsToolkit.createQueue(new CreateQueueRequest("filesPerWorker")).getQueueUrl();
            //todo: state the num per worker
            sqsToolkit.sendMessage(new SendMessageRequest(queueUrl, args[2]));
        } catch (
                AmazonServiceException e) {
            System.out.println("fails to update the manager of the num of the workers");
        }
        System.out.println("Local side: --- Upload file to S3 ----");
        String fileName = "input-sample-2.txt";
        System.out.println("Local side: --- create bucket  ----");
        newbucket = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase() + ""
                + fileName.replace('\\', '.').replace('/', '.').toLowerCase();
        try {
            if (!s3Toolkit.doesBucketExistV2(newbucket)) {
                s3Toolkit.createBucket(new CreateBucketRequest(newbucket));
                System.out.print(" Upload a file as a new object with ContentType and title specified. \n");
                s3Toolkit.putObject(new PutObjectRequest(newbucket, "inputFile", new File(inputFile)));
                Statement allowPulbicStatment = new Statement(Effect.Allow)
                        .withPrincipals(Principal.AllUsers)
                        .withActions(S3Actions.GetObject)
                        .withResources(new S3ObjectResource(newbucket, "*"));
                s3Toolkit.setBucketPolicy(newbucket, new Policy().withStatements(allowPulbicStatment).toJson());

                AccessControlList controling = s3Toolkit.getBucketAcl(newbucket);
                controling.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
                controling.grantPermission(GroupGrantee.AllUsers, Permission.Read);
                controling.grantPermission(GroupGrantee.AllUsers, Permission.Write);
            } else
                System.out.println("bucket creation failssssssssss");

        } catch (AmazonServiceException aws) {
            System.out.println("bucket creation fails");
        }
        Thread.sleep(30000);

        System.out.print("Local side: --- creating job queue to send tasks  ---\n");
        handleQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest("newTask")).getQueueUrl();
        /*stating the location of the input file of s3 */
        sqsToolkit.sendMessage(new SendMessageRequest(handleQueueUrl, newbucket));

        System.out.println("Local side: --- START MANAGER ----");

        List<Reservation> responseList = ec2Toolkit.describeInstances().getReservations();
        for (Reservation res : responseList)
            for (Instance instance : res.getInstances())
                if (instance.getTags() != null)
                    for (Tag tag : instance.getTags()) {
                        String tagVal = tag.getValue();
                        String state = instance.getState().getName();
                        if (tag.getKey().equals("Name") && tagVal.equals("Manager") && state.equals("running")) {
                            break;
                        }
                    }

        TagSpecification tagSpecification = new TagSpecification().withResourceType("instance").withTags(new Tag().withKey("Name").withValue("Manager"));
        /** prepare a script for the ec2 node to run after launching*/
        String[] s = {
                "#!/bin/bash",
                "sudo yum install -y java-1.8.0-openjdk",
                "export AWS_ACCESS_KEY_ID=ASIAYOIATY4PKTKQFHFR",
                "export AWS_SECRET_ACCESS_KEY=F6jS2SSgKeMfRqNXCB6Ou2X8JtVCggFoaziVRCGi",
                "export AWS_SESSION_TOKEN=FwoGZXIvYXdzEHQaDFGj9Cxmq9bjS1cF1CLGAdBKfCdscCHHrFGs0r7690tGlZpKbEona48dgb5iSrIjIZ6HM3LF6iMXFScWMm+36wEXUx2F8Oa6Ev/Iuo+8ITaBam1vcQ2kveXll0Qm9TplFwBYphhWTFcNb2U+p28yOyjmUlIbMTQ0cItYDVgXe89oRuaVCOXMTRchSbyMo+T40jb2T/sOYbOCVuQOvXJRz6ToZpeAFnZ/Us7VrH+TJuglybwfcALgq7LR5XaY5SqceV775/ZXzNbwWvkJ5j5ZSJCIUfkUxSjel96NBjItllDqrhxmgTwHJtB6uKlgLpbPSDFnRcPaHtE1RcsJ69zb13Tfjw/s0m2j2/C1",
                "export AWS_DEFAULT_REGION=us-east-1",
                "aws s3 cp s3://newjarsbucket/Manager.jar Manager.jar",
                "java -jar Manager.jar"
        };

        /**a string containing the code to run*/
        String script = new String(Base64.encodeBase64(String.join("\n", s).getBytes()), StandardCharsets.UTF_8);

        /** Run an instance on ec2 node*/
        RunInstancesRequest runRequest = new RunInstancesRequest()
                .withImageId("ami-00e95a9222311e8ed")
                .withMaxCount(1)
                .withMinCount(1)
                .withInstanceType(InstanceType.T2Micro)
                .withUserData(script)
                .withTagSpecifications(tagSpecification);
        ec2Toolkit.runInstances(runRequest);

        Thread.sleep(20000);
        System.out.println("the main thread is awake from comma\n");


        System.out.print("Local side: --- Checks an SQS queue for a done task message ---\n");
        boolean isDone = false;
        while (!isDone) {
            /** go over all the urls and find the done task queue*/
            for (final String url : sqsToolkit.listQueues().getQueueUrls()) {
                final String[] queueName = url.split("/");
                if (queueName[queueName.length - 1].contentEquals("doneTask")) {
                    isDone = true;
                    System.out.println("DONE TASK IS HERE AT LOCAL\n");
                    /**sends terminate message is isTerminate is true*/
                    new Thread(() -> {
                        System.out.print("\n*) Sending Terminate request: ");
                        if (isTerminate) {
                            final String queueUrl = sqsToolkit.createQueue(new CreateQueueRequest("terminate")).getQueueUrl();
                            sqsToolkit.sendMessage(new SendMessageRequest(queueUrl, "terminate"));
                        }
                        //todo: what should I do when there is no terminate message?
                    }).start();
                    break;
                }
            }
        }

        System.out.print("Local side: --- Local Application downloads the summary file from S3, creates html output file ---\n");
        Thread.sleep(1000);
        final String doneTaskUrl = sqsToolkit.getQueueUrl(new GetQueueUrlRequest("doneTask")).getQueueUrl();
        final String _bucketName = sqsToolkit.receiveMessage(new ReceiveMessageRequest(doneTaskUrl)).getMessages().get(0).getBody();
        Thread.sleep(3000);
        //create summary file....
        System.out.print("Local side: --- Getting the outputFile.txt from the Bucket ---");
        S3Object object = s3Toolkit.getObject(new GetObjectRequest(newbucket, "outputFile.txt"));
        System.out.print(" create summary file ");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile + ".html"));
            System.err.println("CONVERT TO HTML FINAL");
            writer.write("<!DOCTYPE html>" + "\n");
            writer.write("<html>" + "\n");
            writer.write("<head>" + "\n");
            writer.write("<title>");
            writer.write("Summary File");
            writer.write("</title>" + "\n");
            writer.write("</head>" + "\n");
            writer.write("<body>" + "\n");
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write("<p>" + line + "</p>");
                writer.write('\n');
            }
            writer.write("</body>" + "\n");
            writer.write("</html>" + "\n");
            writer.close();
            reader.close();

        } catch (IOException io) {
            System.err.println("IOException" + io.getMessage());
        }
        //todo: clean framework
        sqsToolkit.deleteQueue(new DeleteQueueRequest(sqsToolkit.getQueueUrl(new GetQueueUrlRequest("doneTask")).getQueueUrl()));
        s3Toolkit.deleteObject(new DeleteObjectRequest(newbucket, "outputFile.txt"));
        s3Toolkit.deleteBucket(newbucket);

    }
}

//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.ec2.AmazonEC2;
//import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
//import com.amazonaws.services.ec2.model.*;
//import com.amazonaws.services.ec2.model.Tag;
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
//
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//
//
//public class LocalApplication {
//    private static final DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
//    private static AmazonEC2 ec2Toolkit;
//    private static AmazonS3 s3Toolkit;
//    private static AmazonSQS sqsToolkit;
//
//    private static final String bucket = "bucketqOghAwn0EhUw2nJLVYExSMXT5dCZXfwC".toLowerCase();
//    private static final String newkey = "inputFile" + new Date().getTime();
//    private static final String localQueueName = "localAppQueue" + new Date().getTime(); // Fetch from manager
//    private static final String managerQueueName = "managerQueueqOghAwn0EhUw2nJLVYExSMXT5dCZXfwC"; // Send to Manager
//    public static String inputFile, outputFile;
//    static boolean isTerminate = false;
//    public static int numOfFilesPerWorker;
//
//    private static String instanceId;
//    private static String localQueueUrl;
//    private static String managerQueueUrl;
//
//    public static void main(String[] args) throws InterruptedException {
//        inputFile = args[0];
//        outputFile = args[1];
//        numOfFilesPerWorker = Integer.parseInt(args[2]);
//        if (args[3].equals("terminate"))
//            isTerminate = true;
//
//        ec2Toolkit = AmazonEC2ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//        s3Toolkit = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//        sqsToolkit = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//        s3Toolkit.createBucket(new CreateBucketRequest(bucket));
//        System.out.println("Local side: --- initial queues ---");
//
//        localQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest(localQueueName)).getQueueUrl();
//        managerQueueUrl = sqsToolkit.createQueue(new CreateQueueRequest(managerQueueName)).getQueueUrl();
//
//        System.out.println("Local side: --- initial LocalApp ---");
//
//        System.out.println("Local side: --- START MANAGER IF NOT STARTED ----");
//
//        List<Reservation> responseList = ec2Toolkit.describeInstances().getReservations();
//        for (Reservation res : responseList)
//            for (Instance instance : res.getInstances())
//                if (instance.getTags() != null)
//                    for (Tag tag : instance.getTags()) {
//                        String tagVal = tag.getValue();
//                        String state = instance.getState().getName();
//                        if (tag.getKey().equals("Name") && tagVal.equals("Manager") && (state.equals("running") || state.equals("pending"))) {
//                            instanceId = instance.getInstanceId();
//                            break;
//                        }
//                    }
//
//        TagSpecification tagSpecification = new TagSpecification().withResourceType("instance").withTags(new Tag().withKey("Name").withValue("Manager"));
//        /** prepare a script for the ec2 node to run after launching*/
//        String[] s = {
//                "#!/bin/bash",
//                "sudo yum install -y java-1.8.0-openjdk",
//                "export AWS_ACCESS_KEY_ID=ASIAYOIATY4PGHFRGC5U",
//                "export AWS_SECRET_ACCESS_KEY=ZOiGFnCHLk23QTUGFoSJOR4R/y+wI4IA1r0CelbJ",
//                "export AWS_SESSION_TOKEN=FwoGZXIvYXdzEHAaDBCtTrlurxm3vjB+9iLGAT5iYXWSk7HcIM9xWGV4WgUP2WC1/qiQuLD7lBCBP4zOHdjICzK9N3nbyCRNroNUVm0YZkuHxpudwfUqhhQ8M/6zcpEynh0Ih/9cKN0Xv29olS0+CBKhCZoER9SUwuFyMKhN1PM9Jre07018OZyoyz+01IAYtWfWK82nwVfNYUZK3L8deKCYmOgiqHwmjWPrST9MlSoF69C0ntAJiS6FlrcAU791PrHLh4fxsZpOCVZlzXTSxzc+HKAU/4MOCK7ip6k5pGkBDyj0qt2NBjIt/oFKMTMAU04C/8xBBQOdjvLR+lIvjbYJQyhbZSFi69nQr48CH/9h6yXMKJDY",
//                "export AWS_DEFAULT_REGION=us-east-1",
//                "aws s3 cp s3://newjarsbucket/Manager.jar Manager.jar",
//                "java -jar Manager.jar"
//        };
//
//        /**a string containing the code to run*/
//        String script = new String(Base64.encodeBase64(String.join("\n", s).getBytes()), StandardCharsets.UTF_8);
//
//        /** Run an instance on ec2 node*/
//        RunInstancesRequest runRequest = new RunInstancesRequest()
//                .withImageId("ami-00e95a9222311e8ed")
//                .withMaxCount(1)
//                .withMinCount(1)
//                .withInstanceType(InstanceType.T2Micro)
//                .withUserData(script)
//                .withTagSpecifications(tagSpecification);
//        ec2Toolkit.runInstances(runRequest);
//
//        instanceId = ec2Toolkit.describeInstances().getReservations().get(0).getInstances().get(0).getInstanceId();
//
//        System.out.println("the main thread is awake from comma\n");
//
//        // Upload file to s3 bucket
//        s3Toolkit.putObject(new PutObjectRequest(bucket, newkey, new File(inputFile)));
//
//        // Send message to Manager Queue
//        System.out.println("Local side: --- Send message to Manager Queue ----");
//
//
//        sqsToolkit.sendMessage(new SendMessageRequest(managerQueueUrl, "newTask " + numOfFilesPerWorker + " " + newkey + " " + localQueueUrl));
//
//        // Get the queue url of the manager
//        getMessage();
//    }
//
//    private static void getMessage() throws InterruptedException {
//        System.out.println("In get Message");
//        boolean check = false;
//        while (!check) {
//            List<Message> messages = sqsToolkit.receiveMessage(new ReceiveMessageRequest(localQueueUrl)).getMessages();
//            for (Message message : messages) {
//                System.out.println("Got Message");
//                String[] tokens = message.getBody().split(" ");
//                System.out.println(message.getBody());
//                if (message.getBody().substring(0, "done_task".length()).equals("done_task")) {
//                    S3Object object = s3Toolkit.getObject(new GetObjectRequest(bucket, tokens[1]));
//                    createSummeryHTML(object);
//                    s3Toolkit.deleteObject(new DeleteObjectRequest(bucket, tokens[1]));
//                    sqsToolkit.deleteMessage(new DeleteMessageRequest(localQueueUrl, message.getReceiptHandle()));
//                    System.out.println("Created html file");
//                    if (isTerminate) {
//                        sqsToolkit.sendMessage(new SendMessageRequest(managerQueueUrl, "new_task terminate " + localQueueUrl));
//                    }
//                }
//                 if (message.getBody().equals("terminate_done")) {
//                     check = true;
//                    break;
//                }
//            }
//        }
//    }
//
//    private static void createSummeryHTML(S3Object object) throws InterruptedException {
//        System.out.print("Local side: --- Local Application downloads the summary file from S3, creates html output file ---\n");
//        Thread.sleep(1000);
//
//        try {
//            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
//            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile + ".html"));
//            writer.write("<html><head><title>New Page</title></head><body>\n");
//            String line = reader.readLine();
//            while (line != null) {
//                writer.write("<p>" + line + "</p>");
//                writer.write('\n');
//                line = reader.readLine();
//            }
//            writer.write("</body>" + "\n");
//            writer.write("</html>" + "\n");
//            writer.close();
//            reader.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}

