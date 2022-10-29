
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;


public class Worker {
    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
    public static AmazonS3 s3Toolkit = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static AmazonSQS sqsToolkit = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
    public static String handleErrors = null;
    static String MsgsUrl = sqsToolkit.createQueue(new CreateQueueRequest("donePDFTask")).getQueueUrl();
    static String queueURL = sqsToolkit.getQueueUrl(new GetQueueUrlRequest("newPDFTask")).getQueueUrl();
    public static int messageReceive = 0;

    public static void main(String[] args) throws Exception {
        System.out.println("-- Worker Start --");
        int rece = 0;
        String convertedFile = null;
        while (true) {
            List<Message> messages = sqsToolkit.receiveMessage(new ReceiveMessageRequest(queueURL)).getMessages();
            if (messages != null && messages.size() > 0) {
                if (messages.get(0) != null) {
                    String[] myTask = (messages.get(0).getBody()).split("\\s+");
                    String fileName = myTask[1];
                    String operation = myTask[0];
//                    new Thread(new Runnable() {
//                        @Override
//                        public void run() {
//                            while (true) {
//                                try {
//                                    sqsToolkit.changeMessageVisibility(new ChangeMessageVisibilityRequest(queueURL, messages.get(0).getReceiptHandle(), 20));
//                                } catch (Exception e) {
//                                    break;
//                                }
//                            }
//                        }
//                    }).start();
                    /**download file if not success => error msg*/
                    String downloadedFile = downloadFile(fileName);
                    /**if dowload succeeded => convertfile if not succeeded => error msg*/
                    if (!downloadedFile.equals("Error"))
                        convertedFile = ConvertFile(operation, downloadedFile);

                    /**if all succeeded upload to s3 and save the location*/
                    if (convertedFile != null && !convertedFile.contentEquals("Error")) {
                        String fileLocation = updateFile(convertedFile);
                        sqsToolkit.sendMessage(new SendMessageRequest(MsgsUrl, operation + "       " + fileName + "       " + fileLocation));
                    } else if (convertedFile.contentEquals("Error") || downloadedFile.contentEquals("Error"))
                        sqsToolkit.sendMessage(new SendMessageRequest(MsgsUrl, operation + "       " + fileName + "       " + handleErrors));
                    else
                        sqsToolkit.sendMessage(new SendMessageRequest(MsgsUrl, operation + "       " + fileName + "       " + "ERROOOR"));
                    System.out.println("Message recieved: " + messageReceive + "\n______________________________________________________________");
                    handleErrors = "";
                    sqsToolkit.deleteMessage(new DeleteMessageRequest(queueURL, messages.get(0).getReceiptHandle()));
//                    donetask = true;
                }
            }
        }
    }

    /**
     * create a bucket file to save all the converted file on it
     */
    private static String updateFile(String outputFile) {
        final String newbucket = "mybucketb2b2";
        String fileLocation = null;
        if (!s3Toolkit.doesBucketExistV2(newbucket)) {
            try {
                s3Toolkit.createBucket(new CreateBucketRequest(newbucket));
            } catch (AmazonServiceException aws) {
                System.out.println("bucket creation fails");
                return null;
            }
        }
        /**Upload a file as a new object with ContentType and title specified*/
        s3Toolkit.putObject(new PutObjectRequest(newbucket, outputFile, new File(outputFile)));
        fileLocation = s3Toolkit.getUrl(newbucket, outputFile).toString();
        return fileLocation;
    }

    private static String downloadFile(String file) {
        System.out.printf("Downloading %s\n", file);
        String fileName = "download.pdf";
        try {
            FileUtils.copyURLToFile(new URL(file), new File(fileName), 10000, 10000);
        } catch (MalformedURLException e) {
            handleErrors = "Error occurs while downloading File";
            return "error";
        } catch (IOException e) {
            handleErrors = "Error occurs while downloading File";
            return "Error";
        } catch (Exception e) {
            handleErrors = "Error occurs while downloading File";
            return "Error";
        }
        return fileName;
    }

    private static String ConvertFile(String operation, String fileName) throws Exception {
        System.out.println(String.format(" --- Converting %s to ---\n", fileName));
        messageReceive++;
        String ConvertedFile = null;
        switch (operation) {
            case "ToImage":
                ConvertedFile = ConvertPdfToImage(fileName);
                break;
            case "ToText":
                ConvertedFile = ConvertPdfToText(fileName);
                break;
            case "ToHTML":
                ConvertedFile = ConvertPdfToHTML(fileName);
                break;
        }
        return ConvertedFile;
    }

    private static String ConvertPdfToImage(String filename) throws Exception {
        String outputFile = "toImage" + Math.random() * 1000 + ".png";
        try {
            //        load an existing PDF document
            PDDocument pdf = PDDocument.load(new File(filename));
//        turning it into images that can then be displayed
            PDFRenderer pdfRendered = new PDFRenderer(pdf);
//        BufferedImage subclass describes an Image with an accessible buffer of image data, only the first page
            BufferedImage image = pdfRendered.renderImageWithDPI(0, 300);
            ;
            //write an image to a file.
            ImageIOUtil.writeImage(image, outputFile, 300);
            pdf.close();
        } catch (Exception e) {
            System.out.println("Error occurs while converting file to image ");
            handleErrors = "Error occurs while converting file to text ";
            return "Error";
        }
        return outputFile;
    }

    private static String ConvertPdfToText(String filename) throws Exception {
        String output = "ToText" + Math.random() * 1000 + ".txt";
        try {
            /**load an existing PDF document,put path to your input PDF file here*/
            PDDocument pdf = PDDocument.load(new File(filename));
            /**strip out all of the text and ignore the formatting*/
            PDFTextStripper pdfContent = new PDFTextStripper();
            /**only the first page*/
            pdfContent.setStartPage(1);
            pdfContent.setEndPage(1);
            String pdfContentText = pdfContent.getText(pdf);
            /**print the formatted representation of objects to the text-output stream
             put path to output text file here write the content to text file*/
            PrintWriter printWriter = new PrintWriter(new FileWriter(output));
            pdf.close();
            printWriter.write(pdfContentText);
            printWriter.close();
        } catch (Exception e) {
            System.out.println("Error occurs while converting file to text");
            handleErrors = "Error occurs while converting file to text";
            return "Error";
        }
        return output;
    }

    private static String ConvertPdfToHTML(String filename) throws Exception {
        String output = "ToHtml" + Math.random() * 1000 + ".html";
        try {
            //        load an existing PDF document
            PDDocument pdf = PDDocument.load(new File(filename));
//            Wrap stripped text in simple HTML
            PDFText2HTML text = new PDFText2HTML();
            text.setStartPage(1);
            text.setEndPage(1);
//            This will return the text of a document
            String content = text.getText(pdf);
//            print the formatted representation of objects to the text-output stream
            PrintWriter printWriter = new PrintWriter(new FileWriter(output));
            printWriter.write(content);
            printWriter.close();
            pdf.close();
        } catch (Exception e) {
            System.out.println("Error occurs while converting file to html");
            handleErrors = "Error occurs while converting file to html";
            return "Error";
        }
        return output;
    }
}



//import com.amazonaws.AmazonServiceException;
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
//import com.amazonaws.regions.Regions;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.CreateBucketRequest;
//import com.amazonaws.services.s3.model.PutObjectRequest;
//import com.amazonaws.services.sqs.AmazonSQS;
//import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
//import com.amazonaws.services.sqs.model.*;
//import org.apache.commons.io.FileUtils;
//import org.apache.pdfbox.pdmodel.PDDocument;
//import org.apache.pdfbox.rendering.PDFRenderer;
//import org.apache.pdfbox.text.PDFTextStripper;
//import org.apache.pdfbox.tools.PDFText2HTML;
//import org.apache.pdfbox.tools.imageio.ImageIOUtil;
//
//import java.awt.image.BufferedImage;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.PrintWriter;
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.util.List;
//
//
//public class Worker {
//    public static DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
//    public static AmazonS3 s3Toolkit = AmazonS3ClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//    public static AmazonSQS sqsToolkit = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).withRegion(Regions.US_EAST_1).build();
//    public static String handleErrors = null;
//    public static int messageReceive = 0;
//
//    public static void main(String[] args) throws Exception {
//        System.out.println("-- Worker Start --");
//        String managerQueueUrl = args[0];
//
//        String convertedFile = null;
//        while (true) {
//            List<Message> messages = sqsToolkit.receiveMessage(new ReceiveMessageRequest(managerQueueUrl)).getMessages();
//            for (Message message : messages) {
//                String[] myTask = (message.getBody()).split(" ");
//                String[] msgtoperform = (myTask[1]).split("\\s+");
//                String fileName = msgtoperform[1];
//                String operation = msgtoperform[0];
////                new Thread(new Runnable() {
////                    @Override
////                    public void run() {
////                        while (true) {
////                            try {
////                                sqsToolkit.changeMessageVisibility(new ChangeMessageVisibilityRequest(managerQueueUrl, message.getReceiptHandle(), 20));
////                            } catch (Exception e) {
////                                break;
////                            }
////                        }
////                    }
////                }).start();
//                /**download file if not success => error msg*/
//                String downloadedFile = downloadFile(fileName);
//                /**if dowload succeeded => convertfile if not succeeded => error msg*/
//                if (!downloadedFile.equals("Error"))
//                    convertedFile = ConvertFile(operation, downloadedFile);
//
//                /**if all succeeded upload to s3 and save the location*/
//                if (convertedFile != null && !convertedFile.contentEquals("Error")) {
//                    String fileLocation = updateFile(convertedFile);
//                    sqsToolkit.sendMessage(new SendMessageRequest(myTask[2], operation + " " + fileName + " " + fileLocation));
//                } else if (convertedFile.contentEquals("Error") || downloadedFile.contentEquals("Error"))
//                    sqsToolkit.sendMessage(new SendMessageRequest(myTask[2], operation + " " + fileName + " " + handleErrors));
//                else
//                    sqsToolkit.sendMessage(new SendMessageRequest(myTask[2], operation + " " + fileName + " " + "WHATEVER"));
//                handleErrors = "";
//                sqsToolkit.deleteMessage(new DeleteMessageRequest(managerQueueUrl, message.getReceiptHandle()));
//            }
//        }
//    }
//
//    /**
//     * create a bucket file to save all the converted file on it
//     */
//    private static String updateFile(String outputFile) {
//        final String newbucket = "mybucketb2b2";
//        String fileLocation;
//        if (!s3Toolkit.doesBucketExistV2(newbucket)) {
//            try {
//                s3Toolkit.createBucket(new CreateBucketRequest(newbucket));
//            } catch (AmazonServiceException aws) {
//                System.out.println("bucket creation fails");
//                return null;
//            }
//        }
//        /**Upload a file as a new object with ContentType and title specified*/
//        s3Toolkit.putObject(new PutObjectRequest(newbucket, outputFile, new File(outputFile)));
//        fileLocation = s3Toolkit.getUrl(newbucket, outputFile).toString();
//        return fileLocation;
//    }
//
//    private static String downloadFile(String file) {
//        System.out.printf("Downloading %s\n", file);
//        String fileName = "download.pdf";
//        try {
//            FileUtils.copyURLToFile(new URL(file), new File(fileName), 10000, 10000);
//        } catch (MalformedURLException e) {
//            handleErrors = "Error occurs while downloading File";
//            return "error";
//        } catch (IOException e) {
//            handleErrors = "Error occurs while downloading File";
//            return "Error";
//        } catch (Exception e) {
//            handleErrors = "Error occurs while downloading File";
//            return "Error";
//        }
//        return fileName;
//    }
//
//    private static String ConvertFile(String operation, String fileName) throws Exception {
//        System.out.printf(" --- Converting %s to ---\n%n", fileName);
//        messageReceive++;
//        String ConvertedFile = null;
//        switch (operation) {
//            case "ToImage":
//                ConvertedFile = ConvertPdfToImage(fileName);
//                break;
//            case "ToText":
//                ConvertedFile = ConvertPdfToText(fileName);
//                break;
//            case "ToHTML":
//                ConvertedFile = ConvertPdfToHTML(fileName);
//                break;
//        }
//        return ConvertedFile;
//    }
//
//    private static String ConvertPdfToImage(String filename) throws Exception {
//        String outputFile = "toImage" + Math.random() * 1000 + ".png";
//        try {
//            //        load an existing PDF document
//            PDDocument pdf = PDDocument.load(new File(filename));
////        turning it into images that can then be displayed
//            PDFRenderer pdfRendered = new PDFRenderer(pdf);
////        BufferedImage subclass describes an Image with an accessible buffer of image data, only the first page
//            BufferedImage image = pdfRendered.renderImageWithDPI(0, 300);
//            //write an image to a file.
//            ImageIOUtil.writeImage(image, outputFile, 300);
//            pdf.close();
//        } catch (Exception e) {
//            System.out.println("Error occurs while converting file to image ");
//            handleErrors = "Error occurs while converting file to text ";
//            return "Error";
//        }
//        return outputFile;
//    }
//
//    private static String ConvertPdfToText(String filename) throws Exception {
//        String output = "ToText" + Math.random() * 1000 + ".txt";
//        try {
//            /**load an existing PDF document,put path to your input PDF file here*/
//            PDDocument pdf = PDDocument.load(new File(filename));
//            /**strip out all of the text and ignore the formatting*/
//            PDFTextStripper pdfContent = new PDFTextStripper();
//            /**only the first page*/
//            pdfContent.setStartPage(1);
//            pdfContent.setEndPage(1);
//            String pdfContentText = pdfContent.getText(pdf);
//            /**print the formatted representation of objects to the text-output stream
//             put path to output text file here write the content to text file*/
//            PrintWriter printWriter = new PrintWriter(new FileWriter(output));
//            pdf.close();
//            printWriter.write(pdfContentText);
//            printWriter.close();
//        } catch (Exception e) {
//            System.out.println("Error occurs while converting file to text");
//            handleErrors = "Error occurs while converting file to text";
//            return "Error";
//        }
//        return output;
//    }
//
//    private static String ConvertPdfToHTML(String filename) throws Exception {
//        String output = "ToHtml" + Math.random() * 1000 + ".html";
//        try {
//            //        load an existing PDF document
//            PDDocument pdf = PDDocument.load(new File(filename));
////            Wrap stripped text in simple HTML
//            PDFText2HTML text = new PDFText2HTML();
//            text.setStartPage(1);
//            text.setEndPage(1);
////            This will return the text of a document
//            String content = text.getText(pdf);
////            print the formatted representation of objects to the text-output stream
//            PrintWriter printWriter = new PrintWriter(new FileWriter(output));
//            printWriter.write(content);
//            printWriter.close();
//            pdf.close();
//        } catch (Exception e) {
//            System.out.println("Error occurs while converting file to html");
//            handleErrors = "Error occurs while converting file to html";
//            return "Error";
//        }
//        return output;
//    }
//}

