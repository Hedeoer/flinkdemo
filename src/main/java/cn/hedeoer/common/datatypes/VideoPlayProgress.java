package cn.hedeoer.common.datatypes;

import java.io.Serializable;

public class VideoPlayProgress implements Serializable {
    private long playSec;
    private long positionSec;
    private long videoId;

    public VideoPlayProgress() {}

    public VideoPlayProgress(long playSec, long positionSec, long videoId) {
        this.playSec = playSec;
        this.positionSec = positionSec;
        this.videoId = videoId;
    }

    public long getPlaySec() {
        return playSec;
    }

    public void setPlaySec(long playSec) {
        this.playSec = playSec;
    }

    public long getPositionSec() {
        return positionSec;
    }

    public void setPositionSec(long positionSec) {
        this.positionSec = positionSec;
    }

    public long getVideoId() {
        return videoId;
    }

    public void setVideoId(long videoId) {
        this.videoId = videoId;
    }

    @Override
    public String toString() {
        return "VideoPlayProgress{" +
                "playSec=" + playSec +
                ", positionSec=" + positionSec +
                ", videoId=" + videoId +
                '}';
    }
}
