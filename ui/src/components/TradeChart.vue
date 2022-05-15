<template>
<div class="chart_parent">
    <div id="chart" class="chart">
        <TradeHeader v-bind:open="header.open" :close="header.close" 
        :high="header.high" :low="header.low" :isGrow="header.isGrow"/>
    </div>
    <div class="chart_news">
        <NewsTableDate class="NewsTable" v-bind:date="news_date"/>
    </div>
</div>
</template>
<script>
import { createChart } from 'lightweight-charts';
import TradeHeader from '../components/TradeHeader.vue';
import NewsTableDate from '../components/NewsTableDate.vue';

export default {
    name: 'TradeChart',
    components:{
        TradeHeader,
        NewsTableDate
    },
    data(){
        return{        
            mapped_data: {},
            header: {
                high: '0111',
                low: '0222',
                open: '0333',
                close: '0444',
                isGrow: true
            },
            news_date: '2022-5-11'
        }
    },
    mounted(){
        fetch("http://127.0.0.1:8000/api/trade", {
            mode: 'cors',
            method: 'GET'
        })
        .then(response => response.json())
        .then(data => {
        
        const chart = createChart(document.getElementById("chart"),{
            width: 800,
            height: 700}
        );

        const lineSeries = chart.addCandlestickSeries();//console.log(params['time'])

        chart.subscribeCrosshairMove(params => {
            let currentData = this.mapped_data.filter(i => i.time.year === params['time'].year && i.time.month === params['time'].month && i.time.day === params['time'].day)[0];
            this.header.high = currentData.high.toString();
            this.header.low = currentData.low.toString();
            this.header.open = currentData.open.toString();
            this.header.close = currentData.close.toString();
            this.header.isGrow = currentData.open < currentData.close ? true : false;
        });

        chart.subscribeClick(params => {
            this.news_date = params['time'].year + '-' + params['time'].month + '-' + params['time'].day;
        })

            const mapped_data = data['trade'].map(i => {
                return {time: i['date'], 
                        close: i['close'], 
                        open: i['open'],
                        high: i['high'],
                        low: i['low']
                }
            })

            lineSeries.setData(mapped_data);
            this.mapped_data = mapped_data;
        });

    }
}

</script>

<style>
.NewsTable{
  width:600px;
  overflow: hidden;
    overflow-y: scroll;
    height:100%;
  max-height: 700px;
}

.chart{
    height: 700px;
    width: 800px;
}

.chart_parent{
    display: flex;
    flex-direction: row;
}

.chart_news{
    overflow: hidden;
    
}

</style>