<template>
  <div class="hello">
    <NewsTableBig class="NewsTable"/>
  </div>
</template>

<script>
import NewsTableBig from '../components/NewsTableBig.vue'

export default {
  name: 'NewsPage',
  components:{
    NewsTableBig
  },
  props: {
    msg: String
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
.NewsTable{
  width:600px;
}

.hello{
  display: flex;
  justify-content: center;
  align-items: center;
  overflow-y:hidden;
  max-height: 100%;
}

h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>
