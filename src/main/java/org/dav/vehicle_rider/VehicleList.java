package org.dav.vehicle_rider;

import java.io.Serializable;
import java.util.List;

public class VehicleList implements Serializable {

    private static final long serialVersionUID = 1L;

    private String _vehicles;
    private  String _cashPrefix;
    private  byte _searchPrefixLength;

    public VehicleList (List<VehicleDetails> vehicles, String cashPrefix, byte searchPrefixLength) {
        this._vehicles = toJsonString(vehicles);
        this._cashPrefix = cashPrefix;
        this._searchPrefixLength = searchPrefixLength;
    }

    public String toJsonString(List<VehicleDetails> vehicles) {
        String jsonString = "[";
        for (VehicleDetails vehicle: vehicles) {
            jsonString = jsonString + vehicle.toJSONString() + ",";
        }
        if (vehicles.isEmpty()) {
            jsonString += "]";
        } else {
            jsonString = jsonString.substring(0, jsonString.length() - 1) + "]";
        }
        return jsonString;
    }

    public String getCashPrefix() {
        return _cashPrefix;
    }

    public String getVehicles() {
        return _vehicles;
    }

    public byte getSearchPrefixLength() {
        return _searchPrefixLength;
    }
}
