package cn.yintech.flink;

import com.alibaba.fastjson.annotation.JSONField;

import java.io.Serializable;

public class SensorsEvent implements Serializable {
    private static final long serialVersionUID = -8208517319947843276L;


    private boolean _hybrid_h5;
    private Long _track_id;
    private String event;
    private Long time;
    private Long _flush_time;
    private String distinct_id;
    private String lib;
    private Properties properties;
    private String type;
    private String map_id;
    private Long user_id;
    private Long recv_time;
    private String extractor;
    private Long project_id;
    private String project;
    private Long ver;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    class Properties implements Serializable{

        private static final long serialVersionUID = -2075636029716471758L;

        private String userID;
        private String deviceId;
        private String v1_custom_params;
        private String v1_message_id;
        private String v1_lcs_id;
        private String v1_message_title;
        private String v1_element_content;
        private String v1_custom_params2;

        public String getUserID() {
            return userID;
        }

        public void setUserID(String userID) {
            this.userID = userID;
        }

        public String getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(String deviceId) {
            this.deviceId = deviceId;
        }

        public String getV1_custom_params() {
            return v1_custom_params;
        }

        public void setV1_custom_params(String v1_custom_params) {
            this.v1_custom_params = v1_custom_params;
        }

        public String getV1_message_id() {
            return v1_message_id;
        }

        public void setV1_message_id(String v1_message_id) {
            this.v1_message_id = v1_message_id;
        }

        public String getV1_lcs_id() {
            return v1_lcs_id;
        }

        public void setV1_lcs_id(String v1_lcs_id) {
            this.v1_lcs_id = v1_lcs_id;
        }

        public String getV1_message_title() {
            return v1_message_title;
        }

        public void setV1_message_title(String v1_message_title) {
            this.v1_message_title = v1_message_title;
        }

        public String getV1_element_content() {
            return v1_element_content;
        }

        public void setV1_element_content(String v1_element_content) {
            this.v1_element_content = v1_element_content;
        }

        public String getV1_custom_params2() {
            return v1_custom_params2;
        }

        public void setV1_custom_params2(String v1_custom_params2) {
            this.v1_custom_params2 = v1_custom_params2;
        }

        @Override
        public String toString() {
            return "Properties{" +
                    "userID='" + userID + '\'' +
                    ", deviceId='" + deviceId + '\'' +
                    ", v1_custom_params='" + v1_custom_params + '\'' +
                    ", v1_message_id='" + v1_message_id + '\'' +
                    ", v1_lcs_id='" + v1_lcs_id + '\'' +
                    ", v1_message_title='" + v1_message_title + '\'' +
                    ", v1_element_content='" + v1_element_content + '\'' +
                    ", v1_custom_params2='" + v1_custom_params2 + '\'' +
                    '}';
        }
    }

    public boolean is_hybrid_h5() {
        return _hybrid_h5;
    }

    public void set_hybrid_h5(boolean _hybrid_h5) {
        this._hybrid_h5 = _hybrid_h5;
    }

    public Long get_track_id() {
        return _track_id;
    }

    public void set_track_id(Long _track_id) {
        this._track_id = _track_id;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Long get_flush_time() {
        return _flush_time;
    }

    public void set_flush_time(Long _flush_time) {
        this._flush_time = _flush_time;
    }

    public String getDistinct_id() {
        return distinct_id;
    }

    public void setDistinct_id(String distinct_id) {
        this.distinct_id = distinct_id;
    }

    public String getLib() {
        return lib;
    }

    public void setLib(String lib) {
        this.lib = lib;
    }



    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMap_id() {
        return map_id;
    }

    public void setMap_id(String map_id) {
        this.map_id = map_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getRecv_time() {
        return recv_time;
    }

    public void setRecv_time(Long recv_time) {
        this.recv_time = recv_time;
    }

    public String getExtractor() {
        return extractor;
    }

    public void setExtractor(String extractor) {
        this.extractor = extractor;
    }

    public Long getProject_id() {
        return project_id;
    }

    public void setProject_id(Long project_id) {
        this.project_id = project_id;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public Long getVer() {
        return ver;
    }

    public void setVer(Long ver) {
        this.ver = ver;
    }

    @Override
    public String toString() {
        return "SensorsEvent{" +
                "_hybrid_h5=" + _hybrid_h5 +
                ", _track_id=" + _track_id +
                ", event='" + event + '\'' +
                ", time=" + time +
                ", _flush_time=" + _flush_time +
                ", distinct_id='" + distinct_id + '\'' +
                ", lib='" + lib + '\'' +
                ", properties='" + properties + '\'' +
                ", type='" + type + '\'' +
                ", map_id='" + map_id + '\'' +
                ", user_id=" + user_id +
                ", recv_time=" + recv_time +
                ", extractor='" + extractor + '\'' +
                ", project_id=" + project_id +
                ", project='" + project + '\'' +
                ", ver=" + ver +
                '}';
    }
}
