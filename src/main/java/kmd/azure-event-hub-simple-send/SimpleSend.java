package kmd.AzureEventHubSimpleSend;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class SimpleSend {

	// Specify the telemetry to send to your hub.
	private static class Telemetry {
		
		String deviceOwner;
		int deviceID;
		int rowID;
		double temperature;
		double pulse;
		double steps;
		double meters;

		// Serialize object to JSON format.
		public String serialize() {
			Gson gson = new Gson();
			return gson.toJson(this);
		}

	}

	public static void main(String[] args) 
			throws EventHubException, ExecutionException, InterruptedException, IOException {
		
		// Event Hub connection details:
		String namespace = "KMDEventhub";
		String eventhub = "eventhub1";
		String saskeyname = "RootManageSharedAccessKey";
		String saskey = "Mn0xv7Mbm4N/3g0An653hFAOKdI7/gWz7mpqXgsPOpU=";

		// Telemetry data:
		String deviceOwner = "borgernavn1";
		int deviceID = 11111;
		int rowID = 0;
		double temperature = 36.5;
		double pulse = 65;
		double steps = 0;
		double meters = 0;
		int i = 0;

		Random rand = new Random();
		Telemetry telemetry = new Telemetry();

		ConnectionStringBuilder connStr = new ConnectionStringBuilder().setNamespaceName(namespace)
				.setEventHubName(eventhub).setSasKeyName(saskeyname).setSasKey(saskey);

		while (true) {
			
			i = i++; 
			rowID = i;
			double sendTemperature = temperature + rand.nextDouble();
			double sendPulse = Math.round(pulse + rand.nextDouble() * 10);
			steps = steps + rand.nextDouble() * 5;
			meters = 0.5 * (steps + rand.nextDouble() * 5); // Denne person går 0.5 meter pr. skridt..

			final Gson gson = new GsonBuilder().create();

			// The Executor handles all asynchronous tasks and this is passed to the
			// EventHubClient instance.
			// This enables the user to segregate their thread pool based on the work load.
			// This pool can then be shared across multiple EventHubClient instances.
			// The following sample uses a single thread executor, as there is only one
			// EventHubClient instance,
			// handling different flavors of ingestion to Event Hubs here.
			
			ExecutorService executorService = Executors.newSingleThreadExecutor();

			// Each EventHubClient instance spins up a new TCP/SSL connection, which is
			// expensive.
			// It is always a best practice to reuse these instances. The following sample
			// shows this.
			
			EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);
			
			try {

				System.out.println("Connecting to Event Hub: " + eventhub);
				
				telemetry.deviceOwner = deviceOwner;
				telemetry.deviceID = deviceID;
				telemetry.rowID = rowID;
				telemetry.temperature = sendTemperature;
				telemetry.pulse = sendPulse;
				telemetry.steps = steps;
				telemetry.meters = meters;

				String payload = telemetry.serialize();

				byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
				EventData sendEvent = EventData.create(payloadBytes);

				System.out.println("Sending message: " + payload);

				// Send - not tied to any partition
				// Event Hubs service will round-robin the events across all Event Hubs
				// partitions.
				// This is the recommended & most reliable way to send to Event Hubs.
				
				ehClient.sendSync(sendEvent);

				// System.out.println(payload);
				// System.out.println(sendEvent);
				// System.out.println(payloadBytes);

				System.out.println("Send Complete.");

			} finally {
				ehClient.closeSync();
				executorService.shutdown();

				Thread.sleep(1000);

			}
		}
	}
}