<template>
    <div class="news_item">
        <header>{{author}} - {{timestamp}}</header>
        <main>
            <div>
                <h6>{{title}}</h6>
                <article>
                    {{description}}
                </article>
            </div>
            <img v-bind:src="image_url" class="news_image">
        </main>
    </div>
</template>

<script>
export default {
    props: {
        title: String,
        author: String,
        timestamp: String,
        image_url: String,
        description: String
    },
    name: 'NewsRow'
}
</script>

<style>
.news_image{
    max-height: 100px;
    max-width: 100px;
}

.news_item > main{
    display: flex;
    flex-direction: row;
}

.news_item > main > div{
    flex-grow: 1;
    margin-left: 10px;
    margin-top: 7px;
    margin-right: 10px;
}

.news_item > main > div > h6{
    text-align: left;
    margin: 0px;
    margin-top: 5px;
    font-size: 1.2em;
}

.news_item > main > div > article{
    text-align: left;
    margin-top: 7px;
}

.news_item > header{
    font-size: 0.8em;
    text-align: left;
    color: #7c7c7c;
    padding-left: 10px;
}

.news_item{
    background-color: #ffffff;
    border-radius: 10px;
    padding: 20px;
    margin-top: 10px;
}

.news_item_header{
    text-align: left;
}

.news_item_footer{
    display: flex; 
    flex-direction: row;
}

.news_item_author{
    flex-grow: 0;
    align-self: flex-end;
    color: #7c7c7c;
    font-size: 0.8em;
    justify-self: flex-start;
}

.news_item_space{
    flex-grow: 1;
    width: 1px;
}

.news_item_timestamp{
    flex-grow: 0;
    color: #7c7c7c;
    font-size: 0.8em;
}
</style>