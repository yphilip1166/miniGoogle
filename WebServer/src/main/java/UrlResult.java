import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UrlResult implements Comparable<UrlResult> {
    public float score;
    public String url;
    public Map<String, IndexingItem> wordmap;
    public List<IndexingItem> relatedWord;
    public float pageRank;
    public float neighborScore = -1;
    public UrlResult(float score, String url, float pageRank){
        this.score = score;
        this.url = url;
        this.pageRank = pageRank;
        this.relatedWord = new ArrayList<IndexingItem>();
        this.wordmap = new HashMap<>();
        this.neighborScore = -1;

    }
    public void putWord(IndexingItem item){
        if(!wordmap.containsKey(item.word)){
            wordmap.put(item.word, item);
        }
        relatedWord.add(item);
    }
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder("URL: "+url+", FinalScore: "+score+", PageRank: "+pageRank+", neighborScore: "+neighborScore +", related words:\n");
        for(IndexingItem item : relatedWord){
            sb.append(item.toString());
        }
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public int compareTo(UrlResult to){
        if(this.score == to.score)return 0;
        else if(this.score > to.score)return -1;
        else return 1;
    }

    private float getMinDistance(IndexingItem x, IndexingItem y){
        if(x.word.contains(y.word))return 100000000.0f;
        int[] a = new int[10];
        a[0]=x.p1;
        a[1]=x.p2;
        a[2]=x.p3;
        a[3]=x.p4;
        a[4]=x.p5;
        a[5]=y.p1;
        a[6]=y.p2;
        a[7]=y.p3;
        a[8]=y.p4;
        a[9]=y.p5;
        float minDis = 100000000.0f;
        for(int i = 0; i < 5; i++){
            if(a[i]==-1)break;
            for(int j = 5; j < 10; j++){
                if(a[j]==-1)
                    break;
                int d = Math.abs(a[i]-a[j]);
                if(d < minDis)minDis =(float) d;
            }
        }
        return 1/minDis;
    }

    public float getNeighborScore(){
        if(neighborScore!=-1)return neighborScore;
        relatedWord = new ArrayList<>(wordmap.values());
        int numWords = relatedWord.size();
        float score = 0.0f;
        for(int i = 0; i < numWords; i++){
            for(int j = i+1; j < numWords; j++){
                score += getMinDistance(relatedWord.get(i), relatedWord.get(j));
            }
        }
        neighborScore = score;
        return neighborScore;
    }
}
