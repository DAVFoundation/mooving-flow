package org.dav.vehicle_rider.payment;

import com.google.gson.*;
import org.dav.vehicle_rider.BlueSnapRequests.AuthCaptureRequest;
import org.dav.vehicle_rider.BlueSnapRequests.AuthRequest;
import org.dav.vehicle_rider.BlueSnapRequests.AuthReversalRequest;
import org.slf4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Base64;

public class BlueSnapAPI implements Serializable {
    private final Logger _log;

    private static final long serialVersionUID = 1L;

    private static final String requestPath = "/services/2/transactions";

    private String apiUser;
    private String apiPass;
    private String apiUrl;

    public BlueSnapAPI(String apiUser, String apiPass, String apiDomain, Logger _log) {
        this._log = _log;
        this.apiUser = apiUser;
        this.apiPass = apiPass;
        this.apiUrl = String.format("%s%s", apiDomain, requestPath);
    }

    public BlueSnapAPI(String apiUser, String apiPass, String apiDomain, Logger _log, String requestPath) {
        this(apiUser, apiPass, apiDomain,  _log);
        this.apiUrl = String.format("%s%s", apiDomain, requestPath);
    }

    public String createAuthTxn(String vaultedShopperId, BigDecimal amount, String currency)
            throws MalformedURLException, IOException {
        AuthRequest request = new AuthRequest(vaultedShopperId, currency, amount);
        Gson gson = new Gson();
        String requestString = gson.toJson(request);
        return createTxn(requestString, "POST");
    }

    public String createAuthReversalTxn(String transactionId) throws MalformedURLException, IOException {
        AuthReversalRequest request = new AuthReversalRequest(transactionId);
        Gson gson = new Gson();
        String requestString = gson.toJson(request);
        return createTxn(requestString, "PUT");
    }

    public String createAuthAndCaptureTxn(String vaultedShopperId, String currency, BigDecimal amount, String vendor,
            BigDecimal commissionPercent) throws MalformedURLException, IOException {
        AuthCaptureRequest request = new AuthCaptureRequest(vaultedShopperId, currency, amount, vendor,
                commissionPercent);
        Gson gson = new Gson();
        String requestString = gson.toJson(request);
        return createTxn(requestString, "POST");
    }

    public BlueSnapInfoResponse getPaymentInfo(String vaultedShopperId) throws MalformedURLException, IOException {
        Gson gson = new Gson();
        String responseString = WriteAPI(apiUrl + vaultedShopperId, "GET", null);
        if (responseString == null) {
            return null;
        }
        JsonElement jelement = new JsonParser().parse(responseString);
        JsonObject jobject = jelement.getAsJsonObject();
        JsonObject paymentSources = jobject.getAsJsonObject("paymentSources");
        JsonArray creditCardsInfo = paymentSources.getAsJsonArray("creditCardInfo");
        JsonObject creditCard = (JsonObject) creditCardsInfo.get(0);
        creditCard = creditCard.getAsJsonObject("creditCard");
        BlueSnapInfoResponse response = gson.fromJson(creditCard, BlueSnapInfoResponse.class);
        return response;
    }

    private String createTxn(String request, String method) throws MalformedURLException, IOException {
        Gson gson = new Gson();
        String responseString = WriteAPI(apiUrl, method, request);
        if (responseString == null) {
            return null;
        }
        BlueSnapResponse response = gson.fromJson(responseString, BlueSnapResponse.class);
        return response.transactionId;
    }

    private String WriteAPI(String url, String method, String body) throws MalformedURLException, IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod(method);
        String authHash = Base64.getEncoder().encodeToString(String.format("%s:%s", apiUser, apiPass).getBytes());
        connection.setRequestProperty("Authorization", String.format("Basic %s", authHash));
        connection.setRequestProperty("Content-Type", "application/json");
        if (body != null) {
            byte[] outputInBytes = body.getBytes("UTF-8");
            OutputStream os = connection.getOutputStream();
            os.write(outputInBytes);
            os.close();
        }
        int status = connection.getResponseCode();
        if (status == 200) {
            BufferedReader contentRdr = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder content = new StringBuilder();
            String inputLine;
            while ((inputLine = contentRdr.readLine()) != null)
                content.append(inputLine);
            contentRdr.close();
            return content.toString();
        } else {
            _log.info(String.format("got %s response from bluesnap for url %s, body %s", status, apiUrl, body));
            return null;
        }
    }
}
