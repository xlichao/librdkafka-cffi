# 文字要求
* 在交互和注释、以及代码文档中请使用中文
* 请尽量标注清晰 typehint ：有必要的话可以使用一些内部类型，例如 Message 
# 工程要求
* 对外暴露的类名统一为 KafkaConsumer 和 KafkaProducer
* 在工程里面应该做合理的包装：外部调用的代码不应该再去 import cffi 和 librdkafka 的常量：这些都应该收集到工程代码里面。
* 使用 docker compose 的时候记得加上 sudo 