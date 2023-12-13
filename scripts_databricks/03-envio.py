from slack_sdk import WebClient
import pyspark.pandas as ps


slack_token = "<CHAVE-TOKEN-SLACK>"
client = WebClient(token=slack_token)

nome_arquivo = dbutils.fs.ls("dbfs:/databricks-results/ludo/prata/posts/")[-1].name

path = "../../dbfs/databricks-results/ludo/prata/posts/"+nome_arquivo

enviando_arquivo_csv = client.files_upload_v2(
    channel="<CHANNEL-SLACK-ID>",  
    title="Arquivo no formato CSV dos posts do site ludopedia",
    file=path,
    filename="posts.csv",
    initial_comment="Segue anexo o arquivo CSV:",
)


df_topic_group = ps.read_csv("dbfs:/databricks-results/ludo/prata/topic_group/")
df_user_name = ps.read_csv("dbfs:/databricks-results/ludo/prata/user_name/")
df_topic_name = ps.read_csv("dbfs:/databricks-results/ludo/prata/topic_name/")


# !mkdir imagens -------> Use esse comando dentro do notebook

for topic in df_topic_group.columns[1:]:
    fig = df_topic_group.plot.line(x="begin_date",y=topic)
    fig.write_image(f"./imagens/{topic}.png")

fig2 = df_user_name.plot.bar(x="user_name",y='count')
fig2.write_image(f"./imagens/user_name.png")

fig3 = df_topic_name.plot.bar(x="topic_name",y='count')
fig3.write_image(f"./imagens/topic_name.png")

def enviando_imagens(nome):
    enviando_imagens = client.files_upload_v2(
    channel="C069B5ZTX45",  
    title="Enviando imagens",
    file=f"./imagens/{nome}.png"
)

enviando_imagens('topic_name')
enviando_imagens('user_name')

for topic in df_topic_group.columns[1:]:
    enviando_imagens(topic)
