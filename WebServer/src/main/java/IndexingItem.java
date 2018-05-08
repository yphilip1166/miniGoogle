public class IndexingItem {
    String word;
    String url;
    public int tf;
    public float capitalPercent;
    public float titlePercent;
    public float linkPercent;
    public float emphasisPercent;
    public float metaPercent;
    public float headingScore;
    public float positionScore;
    public int p1;
    public int p2;
    public int p3;
    public int p4;
    public int p5;

    public float pageRank;

    // Calculated Variables
    public float scoreSum;

    public IndexingItem(){
        word = "";
        url = "";
        tf = 0;
        capitalPercent = 0.0f;
        titlePercent = 0.0f;
        linkPercent = 0.0f;
        emphasisPercent = 0.0f;
        metaPercent = 0.0f;
        headingScore = 0.0f;
        positionScore = 0.0f;
        pageRank = 0.15f;
        p1=-1;
        p2=-1;
        p3=-1;
        p4=-1;
        p5=-1;
        IndexingSQL sql = getOne();
    }

    IndexingSQL getOne(){
        return null;
    }

    public IndexingItem(String word, String url,
                        int tf,
                        float capitalPercent,
                        float titlePercent,
                        float linkPercent,
                        float emphasisPercent,
                        float metaPercent,
                        float headingScore,
                        float positionScore,
                        int p1,
                        int p2,
                        int p3,
                        int p4,
                        int p5,
                        float scoreSum){
        this.word = word;
        this.url = url;
        this.tf = tf;
        this.capitalPercent = capitalPercent;
        this.titlePercent = titlePercent;
        this.linkPercent = linkPercent;
        this.emphasisPercent = emphasisPercent;
        this.metaPercent = metaPercent;
        this.headingScore = headingScore;
        this.positionScore = positionScore;
        this.p1=p1;
        this.p2=p2;
        this.p3=p3;
        this.p4=p4;
        this.p5=p5;
        this.scoreSum = scoreSum;
        this.pageRank = 0.15f;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append("word: ");
        sb.append(word);
        sb.append(" ,url: ");
        sb.append(url);
        sb.append(" ,tf: ");
        sb.append(tf);
        sb.append(" ,capital ");
        sb.append(capitalPercent);
        sb.append(" ,title ");
        sb.append(titlePercent);
        sb.append(" ,link ");
        sb.append(linkPercent);
        sb.append(" ,emphasis ");
        sb.append(emphasisPercent);
        sb.append(" ,metaPercent ");
        sb.append(metaPercent);
        sb.append(" ,headingScore ");
        sb.append(headingScore);
        sb.append(" ,positionScore ");
        sb.append(positionScore);
        sb.append(" ,scoreSum ");
        sb.append(scoreSum);
        sb.append('\n');
        return sb.toString();
    }
}
