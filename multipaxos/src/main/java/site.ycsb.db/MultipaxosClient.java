package site.ycsb.db;

import org.codehaus.jackson.map.ObjectMapper;
import site.ycsb.*;

import java.io.*;
import java.net.Socket;
import java.util.*;

/**
 *
 */
public class MultipaxosClient extends DB {
  private Socket socket;
  private Config config;
  private PrintWriter writer;
  private BufferedReader reader;
  private int leaderId;

  @Override
  public void init() throws DBException {
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      config = objectMapper.readValue(
          new File("config.json"),
          Config.class);
    } catch (IOException e) {
      System.err.println("Couldn't load config.json");
      System.exit(1);
    }
    leaderId = config.getLeaderId();
    connect();
  }

  private void connect() {
    String serverAddress = config.getServerAddress(leaderId);
    String[] tokens = serverAddress.split(":");
    String ip = tokens[0];
    int port = Integer.parseInt(tokens[1]);


    try {
      if (socket != null) {
        writer.close();
        reader.close();
        socket.close();
      }
    } catch (IOException ignored) {}

    while (true) {
      try {
        socket = new Socket(ip, port);
        writer = new PrintWriter(socket.getOutputStream(), true);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        break;
      } catch(Exception ignored) {}
    }
  }

  //Read a single record
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    String request = "get " + key + "\n";
    try {
      String response = sendRequest(request);
      result.put("field1", new StringByteIterator(response));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  //Perform a range scan
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  //Update a single record
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  //Insert a single record
  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    StringBuilder value = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      value.append(entry.getValue().toString());
    }
    String request = "put " + key + " " + value + "\n";
    try {
      sendRequest(request);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  //Delete a single record
  @Override
  public Status delete(final String table, final String key) {
    return Status.NOT_IMPLEMENTED;
  }

  private String sendRequest(String request) throws Exception {
    writer.write(request);
    writer.flush();

    String result = reader.readLine();
    if (Objects.equals(result, "retry") ||
        Objects.equals(result, "bad command")) {
      throw new Exception();
    } else if (request.startsWith("leader is")) {
      String[] tokens = request.split(" ");
      leaderId = Integer.parseInt(tokens[2]);
      connect();
    }
    return result;
  }
}
