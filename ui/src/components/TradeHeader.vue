<template>
    <div>
        Open: <span :style="[isGrow ? 'color:green' : 'color:red']">{{open}}</span>
        Close: <span :style="[isGrow ? 'color:green' : 'color:red']">{{close}}</span>
        High: <span :style="[isGrow ? 'color:green' : 'color:red']">{{high}}</span>
        Low: <span :style="[isGrow ? 'color:green' : 'color:red']">{{low}}</span>
    </div>
</template>

<script>
export default {
    props: {
        open: String,
        close: String,
        high: String,
        low: String,
        isGrow: Boolean
    },
    // data(){
    //     return {
    //         isGrow: true
    //     }
    // },
    name: 'TradeHeader'
}
</script>

<style>
.chart_header{
    font-size: 1.3em;
}

.chart_header_digit_green{
    color:green;
}

.chart_header_digit_red{
    color:red;
}
</style>
