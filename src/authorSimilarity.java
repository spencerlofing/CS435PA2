
public class authorSimilarity implements Comparable<authorSimilarity>
{
	private String author;
	private Double similarity;
	public authorSimilarity(String author, Double similarity)
	{
		this.author = author;
		this.similarity = similarity;
	}
	
	public String getAuthor()
	{
		return author;
	}
	public Double getSimilarity()
	{
		return similarity;
	}
	
	public int compareTo(authorSimilarity other)
	{
		return 0 - this.getSimilarity().compareTo(other.getSimilarity());
	}
}
