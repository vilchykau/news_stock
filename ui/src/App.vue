<template>
  <header class="app_top_hearder">NEWS_ANALYT</header>
  <div class="central_div">
    <NavigationBar class="menu"/>
    <router-view class="main_cls"/>
  </div>
</template>

<script>
import NavigationBar from './components/NavigationBar.vue'

export default {
  name: 'App',
  components:{
    NavigationBar
  }
}
</script>

<style>
.bigNavButton{
  width: 150px;
  height: 40px;
  border-radius: 20px;
  background-color: #00000000;
  border-color: #71abf2;
  border-style: none;
  color: black;
  font-size: 1.3em;
  margin-top: 10px;
}

.bigNavButton:hover{
  color: #71abf2;
}

.bigNavButton:active{
  border-style: solid;
}

.central_div{
  display: flex; 
  flex-direction: row;
  width: 100%;
  align-items: stretch;
  max-height: 100%;
}

.menu{
  width: 170px;
}

#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
  height: 100%;
}

body{
    background-color:rgb(243, 242, 239);
}

.app_top_hearder{
  background-color: rgb(0, 149, 255);
  margin-top: 0; 
  padding: 0px;
  padding-left: 10px;
  text-align: left;
  color:aliceblue;
  font-style:oblique;
  height: 40px;
  display: flex; 
  flex-direction: row;
  align-items: center;
}

body > #app { 
  margin-top: 0;
  margin-left: 0px;
  margin-right: 0px;
}

body{
  margin-top: 0;
  margin-left: 0px;
  margin-right: 0px;
}

.main_cls{
  flex-grow: 1;
}

</style>
