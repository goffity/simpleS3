package com.goffity.aws.s3.simpleS3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

public class App {
	public static void main(String[] args) throws AmazonServiceException, AmazonClientException, IOException {

		AmazonS3 amazonS3 = AmazonS3Client.builder().withRegion(Regions.AP_SOUTHEAST_1).build();

		// Region region = Region.getRegion(Regions.AP_SOUTHEAST_1);
		// amazonS3.setRegion(region);

		System.out.println("Listing buckets");
		for (Bucket bucket : amazonS3.listBuckets()) {
			System.out.println(" - " + bucket.getName());

			ObjectListing object_listing = amazonS3.listObjects(bucket.getName());
			while (true) {
				for (Iterator<?> iterator = object_listing.getObjectSummaries().iterator(); iterator.hasNext();) {
					S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
					System.out.println("Delete object " + summary.getKey() + " in bucket " + bucket.getName());
					amazonS3.deleteObject(bucket.getName(), summary.getKey());
				}

				// more object_listing to retrieve?
				if (object_listing.isTruncated()) {
					object_listing = amazonS3.listNextBatchOfObjects(object_listing);
				} else {
					break;
				}
			}
			;

			System.out.println(" - removing versions from bucket");
			VersionListing version_listing = amazonS3
					.listVersions(new ListVersionsRequest().withBucketName(bucket.getName()));
			while (true) {
				for (Iterator<?> iterator = version_listing.getVersionSummaries().iterator(); iterator.hasNext();) {
					S3VersionSummary vs = (S3VersionSummary) iterator.next();
					amazonS3.deleteVersion(bucket.getName(), vs.getKey(), vs.getVersionId());
				}

				if (version_listing.isTruncated()) {
					version_listing = amazonS3.listNextBatchOfVersions(version_listing);
				} else {
					break;
				}
			}

			System.out.println("==== Delete bucket ====");
			System.out.println("Bucket name: " + bucket.getName());
			amazonS3.deleteBucket(bucket.getName());

		}
		System.out.println();

		String bucketName = "s3-bucket-" + UUID.randomUUID();
		String key = "testUpload";

		System.out.println("Creating bucket " + bucketName + "\n");
		amazonS3.createBucket(bucketName);

		System.out.println("Uploading a new object to S3 from a file\n");
		amazonS3.putObject(new PutObjectRequest(bucketName, key, createSampleFile()));

		amazonS3.setObjectAcl(bucketName, key, CannedAccessControlList.PublicRead);

		String url = ((AmazonS3Client) amazonS3).getResourceUrl(bucketName, key);
		System.out.println("==== URL ====");
		System.out.println(url);
		System.out.println("=============");

		AccessControlList accessControlList = amazonS3.getBucketAcl(bucketName);
		List<Grant> grants = accessControlList.getGrantsAsList();
		for (Grant grant : grants) {
			System.out.format("  %s: %s\n", grant.getGrantee().getIdentifier(), grant.getPermission().toString());
		}

		accessControlList = amazonS3.getObjectAcl(bucketName, key);
		grants = accessControlList.getGrantsAsList();
		for (Grant grant : grants) {
			System.out.format("Object  %s: %s\n", grant.getGrantee().getIdentifier(), grant.getPermission().toString());
		}
	}

	private static File createSampleFile() throws IOException {
		File file = File.createTempFile("aws-java-sdk-", ".txt");
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.write("01234567890112345678901234\n");
		writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
		writer.write("01234567890112345678901234\n");
		writer.write("abcdefghijklmnopqrstuvwxyz\n");
		writer.close();

		return file;
	}
}
