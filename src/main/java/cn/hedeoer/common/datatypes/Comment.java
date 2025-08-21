package cn.hedeoer.common.datatypes;
// 文章评论
public class Comment {

    private Long id;
    private Long userId;
    private String body;
    private Long postId;
    private Long views;
    private Long status;

    public Comment() {
    }

    public Comment(Long id, Long userId, String body, Long postId, Long views, Long status) {
        this.id = id;
        this.userId = userId;
        this.body = body;
        this.postId = postId;
        this.views = views;
        this.status = status;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    public Long getViews() {
        return views;
    }

    public void setViews(Long views) {
        this.views = views;
    }

    public Long getPostId() {
        return postId;
    }

    public void setPostId(Long postId) {
        this.postId = postId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "id=" + id +
                ", userId=" + userId +
                ", body='" + body + '\'' +
                ", postId=" + postId +
                ", views=" + views +
                ", status=" + status +
                '}';
    }
}
