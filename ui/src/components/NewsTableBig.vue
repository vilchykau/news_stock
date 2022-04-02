<template>
    <div>
        <select name="cars" id="cars">
            <option value="volvo">Volvo</option>
            <option value="saab">Saab</option>
            <option value="mercedes">Mercedes</option>
            <option value="audi">Audi</option>
        </select>
        <NewsRow v-for="(n) in news" v-bind:key="n"
            v-bind:title="n.title" v-bind:author="n.company" v-bind:timestamp="n.publish_timestamp"
            v-bind:image_url="n.image_url" v-bind:description="n.description"/>
    </div>
</template>

<script>
import NewsRow from './NewsRow.vue'

export default {
    name: "NewsTableBig",
    components:{
        NewsRow
    },
    data(){
        return{        news:[
            {
                title:'2002 iMac resurrected with Apple’s M1 chip',
                company:'Nvidia'
            },
            {
                title:'3 Blue-Chip Tech Stocks Wall Street Predicts Will Rally More Than 50%',
                company:'Nvidia'
            }
        ]
        }
    },
    created() {
        fetch("http://127.0.0.1:8000/api/news_list", {
            mode: 'cors',
            method: 'GET'
        })
        .then(response => response.json())
        .then(data => (this.news = data.news));
    }
}
</script>