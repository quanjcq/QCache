package core.message;

public class RaftVoteMessage extends RaftMessage{
    private boolean voteFor = false;

    public boolean isVoteFor() {
        return voteFor;
    }

    public void setVoteFor(boolean voteFor) {
        this.voteFor = voteFor;
    }

    @Override
    public String toString() {
        return super.toString() +
                "voteFor=" + voteFor +
                '}';
    }
}
