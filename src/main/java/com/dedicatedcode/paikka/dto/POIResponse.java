package com.dedicatedcode.paikka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public class POIResponse {
    
    @JsonProperty("id")
    private long id;
    
    @JsonProperty("lat")
    private float lat;
    
    @JsonProperty("lon")
    private float lon;
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("subtype")
    private String subtype;
    
    @JsonProperty("distance_km")
    private double distanceKm;
    
    @JsonProperty("names")
    private Map<String, String> names;
    
    @JsonProperty("display_name")
    private String displayName;
    
    @JsonProperty("address")
    private Map<String, String> address;
    
    @JsonProperty("hierarchy")
    private List<HierarchyItem> hierarchy;
    
    @JsonProperty("boundary")
    private GeoJsonGeometry boundary;
    
    @JsonProperty("query")
    private QueryInfo query;
    
    // Constructors
    public POIResponse() {}
    
    // Getters and setters
    public long getId() { return id; }
    public void setId(long id) { this.id = id; }
    
    public float getLat() { return lat; }
    public void setLat(float lat) { this.lat = lat; }
    
    public float getLon() { return lon; }
    public void setLon(float lon) { this.lon = lon; }
    
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    
    public String getSubtype() { return subtype; }
    public void setSubtype(String subtype) { this.subtype = subtype; }
    
    public double getDistanceKm() { return distanceKm; }
    public void setDistanceKm(double distanceKm) { this.distanceKm = distanceKm; }
    
    public Map<String, String> getNames() { return names; }
    public void setNames(Map<String, String> names) { this.names = names; }
    
    public String getDisplayName() { return displayName; }
    public void setDisplayName(String displayName) { this.displayName = displayName; }
    
    public Map<String, String> getAddress() { return address; }
    public void setAddress(Map<String, String> address) { this.address = address; }
    
    public List<HierarchyItem> getHierarchy() { return hierarchy; }
    public void setHierarchy(List<HierarchyItem> hierarchy) { this.hierarchy = hierarchy; }
    
    public GeoJsonGeometry getBoundary() { return boundary; }
    public void setBoundary(GeoJsonGeometry boundary) { this.boundary = boundary; }
    
    public QueryInfo getQuery() { return query; }
    public void setQuery(QueryInfo query) { this.query = query; }
    
    public static class HierarchyItem {
        @JsonProperty("level")
        private int level;
        
        @JsonProperty("type")
        private String type;
        
        @JsonProperty("name")
        private String name;
        
        @JsonProperty("osm_id")
        private long osmId;
        
        @JsonProperty("geometry_url")
        private String geometryUrl;
        
        public HierarchyItem() {}
        
        public HierarchyItem(int level, String type, String name, long osmId) {
            this.level = level;
            this.type = type;
            this.name = name;
            this.osmId = osmId;
        }
        
        public HierarchyItem(int level, String type, String name, long osmId, String geometryUrl) {
            this.level = level;
            this.type = type;
            this.name = name;
            this.osmId = osmId;
            this.geometryUrl = geometryUrl;
        }
        
        public int getLevel() { return level; }
        public void setLevel(int level) { this.level = level; }
        
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public long getOsmId() { return osmId; }
        public void setOsmId(long osmId) { this.osmId = osmId; }
        
        public String getGeometryUrl() { return geometryUrl; }
        public void setGeometryUrl(String geometryUrl) { this.geometryUrl = geometryUrl; }
    }
    
    public static class QueryInfo {
        @JsonProperty("lat")
        private double lat;
        
        @JsonProperty("lon")
        private double lon;
        
        @JsonProperty("lang")
        private String lang;
        
        public QueryInfo() {}
        
        public QueryInfo(double lat, double lon, String lang) {
            this.lat = lat;
            this.lon = lon;
            this.lang = lang;
        }
        
        public double getLat() { return lat; }
        public void setLat(double lat) { this.lat = lat; }
        
        public double getLon() { return lon; }
        public void setLon(double lon) { this.lon = lon; }
        
        public String getLang() { return lang; }
        public void setLang(String lang) { this.lang = lang; }
    }
}
