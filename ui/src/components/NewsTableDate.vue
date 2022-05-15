<template>
    <div>
        <NewsRow v-for="(n) in news" v-bind:key="n"
            v-bind:title="n.title" v-bind:author="n.company" v-bind:timestamp="n.publish_timestamp"
            v-bind:image_url="n.image_url" v-bind:description="n.description"/>
    </div>
</template>

<script>
import NewsRow from './NewsRow.vue'

export default {
    name: "NewsTableDate",
    props:{
        date: String
    },
    components:{
        NewsRow
    },
    data(){
        return {news:[]};
    },
    created() {
        fetch("http://127.0.0.1:8000/api/news_list_date/" + this.date, {
            mode: 'cors',
            method: 'GET'
        })
        .then(response => response.json())
        .then(data => (this.news = data.news));
    },
    watch: { 
        date: function(newVal, oldVal) { // watch it
            console.log('Prop changed: ', newVal, ' | was: ', oldVal)
            fetch("http://127.0.0.1:8000/api/news_list_date/" + newVal, {
                mode: 'cors',
                method: 'GET'
            })
            .then(response => response.json())
            .then(data => (this.news = data.news));
        }
    }
}
</script>