from wordcloud import WordCloud
import matplotlib.pyplot as plt


def plot_wordcloud(texts, max_words=100, title="Word Cloud"):
    """
    Generate and plot a word cloud from a list of texts.

    Args:
        texts (list[str]): List of strings.
        max_words (int): Maximum words in the cloud.
        title (str): Plot title.
    """
    combined_text = " ".join([str(t) for t in texts if t])

    wc = WordCloud(
        width=800,
        height=400,
        max_words=max_words,
        background_color="white",
        colormap="viridis"
    ).generate(combined_text)

    plt.figure(figsize=(15, 7))
    plt.imshow(wc, interpolation="bilinear")
    plt.axis("off")
    plt.title(title, fontsize=16)
    plt.show()

# Example usage:
# plot_wordcloud(df['txt_clean'].tolist(), max_words=100, title="Bank Reviews Word Cloud")
