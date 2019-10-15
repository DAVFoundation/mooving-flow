package org.dav.vehicle_rider;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.dav.vehicle_rider.LockVehicleFlow.LockVehicleMessage;
import org.dav.vehicle_rider.messages.VehicleMessageWithRideDetails;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.joda.time.LocalDate;
import org.slf4j.Logger;

public class GetRideLocalTime<T extends VehicleMessageWithRideDetails> extends DoFn<T, T> {

    private static final long serialVersionUID = 1L;

    private final Logger _log;
    private HttpsURLConnection _connection;
    private final String _urlFormat = "https://maps.googleapis.com/maps/api/timezone/json?location=%s,%s&timestamp=%s&key=%s";
    private final String _apiKey;

    public GetRideLocalTime(String apiKey, Logger log) {
        _log = log;
        _apiKey = apiKey;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        T lockMessage = context.element();
        double latitude = GeoHashUtils.decodeLatitude(lockMessage.getStartGeoHash());
        double longitude = GeoHashUtils.decodeLongitude(lockMessage.getStartGeoHash());

        long utcLastTimeInSeconds = (lockMessage.getLastTime().getTime() / 1000);
        String fullUrl = String.format(_urlFormat, latitude, longitude, utcLastTimeInSeconds, _apiKey);
        try {
            URL obj = new URL(fullUrl);
            _connection = (HttpsURLConnection) obj.openConnection();
            _connection.setRequestMethod("GET");

            int status = _connection.getResponseCode();
            if (status == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(_connection.getInputStream()));
                String inputLine;
                StringBuffer content = new StringBuffer();
                while ((inputLine = in.readLine()) != null) {
                    content.append(inputLine);
                }
                in.close();
                String response = content.toString();
                _log.info(String.format("response is: %s", response));
                final GsonBuilder builder = new GsonBuilder();
                Gson _gson = builder.create();
                TimeZoneApiResponse apiResponse = _gson.fromJson(response, TimeZoneApiResponse.class);
                long localRideTime = utcLastTimeInSeconds + apiResponse.DstOffset + apiResponse.RawOffset;
                localRideTime *= 1000;
                lockMessage.setEffectiveDate(new LocalDate(localRideTime));
            } else {
                _log.info(String.format("got %s response from timezone api for url %s, status code: %s", status, fullUrl, status));
            }
            context.output(lockMessage);
            _connection.disconnect();
        } catch (Exception ex) {
            _log.error(String.format("error while trying to get local ride time for url: %s", fullUrl), ex);
            context.output(lockMessage);
        }
    }
}