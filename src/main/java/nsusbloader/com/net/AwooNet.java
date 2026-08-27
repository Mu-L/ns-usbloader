/*
    Copyright 2019-2026 Dmitry Isaenko

    This file is part of NS-USBloader.

    NS-USBloader is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    NS-USBloader is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with NS-USBloader.  If not, see <https://www.gnu.org/licenses/>.
*/
package nsusbloader.com.net;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import nsusbloader.ModelControllers.CancellableRunnable;
import nsusbloader.ModelControllers.ILogPrinter;
import nsusbloader.ModelControllers.Log;
import nsusbloader.NSLDataTypes.EFileStatus;
import nsusbloader.NSLDataTypes.EModule;
import nsusbloader.NSLDataTypes.EMsgType;
import nsusbloader.com.helpers.NSSplitReader;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static java.lang.Long.parseLong;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class AwooNet extends CancellableRunnable {

    private static final String LAST_MODIFIED = "Thu, 01 Jan 1970 00:00:00 GMT";
    private final static int SWITCH_PORT = 2000;
    private final static int CHUNK_SIZE = 0x40_00;
    private final static int POLL_MS = 200;          // idle-poll interval for the run() serve loop

    private final ILogPrinter logPrinter;

    private final String switchIp;
    private final String hostIP;
    private final int hostPort;
    private final String extras;
    private final boolean doNotServe;

    private final HashMap<String, UniFile> files;

    private ServerSocket reservedSocket;

    private final boolean isValid;
    private volatile HttpServer server;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;
    private volatile boolean reservedReleased = false;

    public AwooNet(List<File> filesList,
                   String switchIp,
                   boolean doNotServe,
                   String hostIP,
                   String hostPortNum,
                   String extras) {
        this.doNotServe = doNotServe;
        this.extras = doNotServe ? extras : "";
        this.switchIp = switchIp;
        this.logPrinter = Log.getPrinter(EModule.USB_NET_TRANSFERS);

        var validator = new NetworkSetupValidator(filesList, doNotServe, hostIP, hostPortNum, logPrinter);

        this.hostIP = validator.getHostIP();
        this.hostPort = validator.getHostPort();
        this.files = validator.getFiles();
        this.reservedSocket = validator.getServerSocket();
        this.isValid = validator.isValid();

        if (! isValid)
            close(EFileStatus.FAILED);
    }

    @Override
    public void run() {
        if (! isValid || isCancelled())
            return;

        print("\tStart chain", EMsgType.INFO);

        if (sendListOfFiles())
            return;

        if (doNotServe) {
            print("List of files transferred. Replies won't be served.", EMsgType.PASS);
            close(EFileStatus.UNKNOWN);
            return;
        }
        print("Initiation files list has been sent to NS.", EMsgType.PASS);

        serveRequests();
    }

    private boolean sendListOfFiles() {
        try {
            final var prefix = hostIP + ':' + hostPort + '/' + extras;
            var payload = files.keySet().stream()
                    .map(fileName -> prefix + encode(fileName, UTF_8).replace("+", "%20"))
                    .collect(Collectors.joining("\n", "", "\n"))
                    .getBytes(UTF_8);
            var payloadSize = ByteBuffer.allocate(Integer.BYTES)
                    .putInt(payload.length)
                    .array();

            try (var switchSocket = new Socket(switchIp, SWITCH_PORT)) {
                var switchStream = switchSocket.getOutputStream();
                switchStream.write(payloadSize);
                switchStream.write(payload);
                switchStream.flush();
            }
            return false;
        }
        catch (Exception e) {
            print("Unable to connect to NS or send files list:%n         "+e.getMessage(), EMsgType.FAIL);
            close(EFileStatus.UNKNOWN);
            return true;
        }
    }

    private void serveRequests() {
        try {
            releaseReservedSocket();
            var address = new InetSocketAddress(hostIP, hostPort);
            server = HttpServer.create(address, 64);
            server.createContext("/", new FileHandler());
            server.setExecutor(newVirtualThreadPerTaskExecutor());
            server.start();
            print("Serving HTTP requests on %s:%d".formatted(hostIP, hostPort), EMsgType.INFO);

            while (!isCancelled()) {
                if (done.await(POLL_MS, MILLISECONDS))
                    break;
            }
            print("Interrupted", EMsgType.INFO);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            print("Interrupted by user", EMsgType.INFO);
        }
        catch (Exception e) {
            print(e.getMessage(), EMsgType.INFO);
            close(EFileStatus.UNKNOWN);
            return;
        }
        stopServer();
        print("All transfers complete", EMsgType.PASS);
        close(EFileStatus.UPLOADED);
    }

    private void releaseReservedSocket() {
        if (reservedReleased)
            return;
        reservedReleased = true;
        if (reservedSocket != null && !reservedSocket.isClosed()) {
            try {
                reservedSocket.close();
            }
            catch (IOException ignore) {}
            reservedSocket = null;
        }
    }

    private void stopServer() {
        if (stopped)
            return;
        if (server != null) {
            try {
                server.stop(1);
            }
            catch (Exception e) {
                print("Failed to stop HTTP server: "+e.getMessage(), EMsgType.WARNING);
            }
            server = null;
        }
        stopped = true;
    }

    /**
     * HTTP handler dispatched by the built-in {@link HttpServer} for every request.
     * */
    private class FileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) {
            try (exchange) {
                handleRequest(exchange);
            }
            catch (Exception e) {
                print(e.getMessage(), EMsgType.FAIL);
            }
        }
    }

    private void handleRequest(HttpExchange exchange) throws Exception {
        var method = exchange.getRequestMethod();
        if (method.startsWith("DROP")) {
            cancel();
            return;
        }

        var uniFile = lookup(exchange.getRequestURI().getPath(), exchange);
        if (uniFile == null)
            return;

        var fileSize = uniFile.getSize();
        print(method+" "+uniFile, EMsgType.INFO);

        if (method.startsWith("HEAD"))
            replyFull(exchange, fileSize);
        else if (method.startsWith("GET"))
            handleRange(exchange, uniFile.getFile(), fileSize);
    }

    /**
     * Looks up the file by decoded path segment and replies 404 (or 400) if missing/empty.
     */
    private UniFile lookup(String path, HttpExchange exchange) throws Exception {
        if (path == null || path.isBlank()) {
            replyError(exchange, 400);
            return null;
        }
        var uniFile = files.get(path.substring(1));
        if (uniFile == null || !uniFile.getFile().exists() || uniFile.getSize() == 0) {
            print("'%s' doesn't exist or have 0 size. Reply 404".formatted(path), EMsgType.FAIL);
            replyError(exchange, 404);
            return null;
        }
        return uniFile;
    }

    private void handleRange(HttpExchange exchange, File file, long fileSize) throws Exception {
        try {
            var rangeStr = exchange.getRequestHeaders().getFirst("Range")
                    .replaceFirst("^.*bytes=", "")
                    .split("-", 2);

            long start;
            long end;

            var hasStart = !rangeStr[0].isEmpty();
            var hasEnd = !rangeStr[1].isEmpty();
            if (hasStart && hasEnd) {             // "Range: bytes=100-101"
                start = parseLong(rangeStr[0]);
                end = parseLong(rangeStr[1]);
            }
            else if (hasStart) {                  // "Range: bytes=100-"
                start = parseLong(rangeStr[0]);
                end = fileSize-1;
            }
            else if (hasEnd) {                    // "Range: bytes=-101"
                start = fileSize-parseLong(rangeStr[1])-1;
                end = fileSize-1;
            }
            else {                                // "Range: bytes=-"
                print("%s file requested size of %s. Reply 416".formatted(file.getName(), fileSize), EMsgType.FAIL);
                logPrinter.update(file, EFileStatus.FAILED);
                replyError(exchange, 416);
                return;
            }

            if (start > end) {
                print("Requested Range Not Satisfiable. Reply 416", EMsgType.FAIL);
                logPrinter.update(file, EFileStatus.FAILED);
                replyError(exchange, 416);
                return;
            }

            print("    0x%x-0x%x | %d-%d".formatted(start, end, start, end), EMsgType.INFO);
            serveSlice(exchange, file, fileSize, start, end);
        }
        catch (NumberFormatException nfe) {
            print("Request for "+file.getName()+" has incorrect format. Reply 400\n\t"+nfe.getMessage(),
                    EMsgType.FAIL);
            logPrinter.update(file, EFileStatus.FAILED);
            replyError(exchange, 400);
        }
    }

    private void serveSlice(HttpExchange exchange, File file, long fileSize, long start, long end) throws Exception {
        var length = end - start + 1;
        var headers = exchange.getResponseHeaders();
        headers.set("Server", "NS-USBloader");
        headers.set("Content-Type", "application/octet-stream");
        headers.set("Accept-Ranges", "bytes");
        headers.set("Content-Range", "bytes %d-%d/%d".formatted(start, end, fileSize));
        headers.set("Last-Modified", LAST_MODIFIED);
        exchange.sendResponseHeaders(206, length);
        writeRange(file, start, length, exchange.getResponseBody());
        logPrinter.updateProgress(1.0);
    }

    private void replyFull(HttpExchange exchange, long fileSize) throws Exception {
        var headers = exchange.getResponseHeaders();
        headers.set("Server", "NS-USBloader");
        headers.set("Content-Type", "application/octet-stream");
        headers.set("Accept-Ranges", "bytes");
        headers.set("Content-Range", "bytes 0-%d/%d".formatted(fileSize - 1, fileSize));
        headers.set("Content-Length", Long.toString(fileSize));
        headers.set("Last-Modified", LAST_MODIFIED);
        exchange.sendResponseHeaders(200, -1);
    }

    private void replyError(HttpExchange exchange, int code) throws Exception {
        var headers = exchange.getResponseHeaders();
        headers.set("Server", "NS-USBloader");
        headers.set("Connection", "close");
        headers.set("Content-Type", "text/html;charset=utf-8");
        exchange.sendResponseHeaders(code, -1);
    }

    private void writeRange(File file, long start, long count, OutputStream out) throws Exception {
        try (var in = file.isDirectory()
                ? new NSSplitReader(file, start)
                : new BufferedInputStream(new FileInputStream(file))) {
            if (!file.isDirectory() && in.skip(start) != start)
                throw new IOException("Unable to skip requested range.");

            var written = 0L;
            var buf = new byte[CHUNK_SIZE];
            while (written < count) {
                var chunk = toIntExact(min(buf.length, count - written));
                var read = in.read(buf, 0, chunk);
                if (read < 0)
                    throw new IOException("File stream suddenly ended.");
                out.write(buf, 0, read);
                written += read;
                logPrinter.updateProgress((double) written / count);
            }
            out.flush();
        }
    }

    private void close(EFileStatus status) {
        releaseReservedSocket();
        stopServer();

        var tempMap = files.values().stream().collect(Collectors.toMap(
                uniFile -> uniFile.getFile().getName(),
                uniFile -> uniFile.getFile()));

        logPrinter.update(tempMap, status);
        print("\tEnd chain", EMsgType.INFO);
        logPrinter.close();
    }

    private void print(String message, EMsgType type) {
        try {
            logPrinter.print(message, type);
        }
        catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        super.cancel();
        done.countDown();
    }
}