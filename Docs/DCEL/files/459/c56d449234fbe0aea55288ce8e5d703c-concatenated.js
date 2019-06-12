
/** javascripts/vendor/jquery-1.9.0.min.js **/
/*! jQuery v1.9.0 | (c) 2005, 2012 jQuery Foundation, Inc. | jquery.org/license */(function(e,t){"use strict";function n(e){var t=e.length,n=st.type(e);return st.isWindow(e)?!1:1===e.nodeType&&t?!0:"array"===n||"function"!==n&&(0===t||"number"==typeof t&&t>0&&t-1 in e)}function r(e){var t=Tt[e]={};return st.each(e.match(lt)||[],function(e,n){t[n]=!0}),t}function i(e,n,r,i){if(st.acceptData(e)){var o,a,s=st.expando,u="string"==typeof n,l=e.nodeType,c=l?st.cache:e,f=l?e[s]:e[s]&&s;if(f&&c[f]&&(i||c[f].data)||!u||r!==t)return f||(l?e[s]=f=K.pop()||st.guid++:f=s),c[f]||(c[f]={},l||(c[f].toJSON=st.noop)),("object"==typeof n||"function"==typeof n)&&(i?c[f]=st.extend(c[f],n):c[f].data=st.extend(c[f].data,n)),o=c[f],i||(o.data||(o.data={}),o=o.data),r!==t&&(o[st.camelCase(n)]=r),u?(a=o[n],null==a&&(a=o[st.camelCase(n)])):a=o,a}}function o(e,t,n){if(st.acceptData(e)){var r,i,o,a=e.nodeType,u=a?st.cache:e,l=a?e[st.expando]:st.expando;if(u[l]){if(t&&(r=n?u[l]:u[l].data)){st.isArray(t)?t=t.concat(st.map(t,st.camelCase)):t in r?t=[t]:(t=st.camelCase(t),t=t in r?[t]:t.split(" "));for(i=0,o=t.length;o>i;i++)delete r[t[i]];if(!(n?s:st.isEmptyObject)(r))return}(n||(delete u[l].data,s(u[l])))&&(a?st.cleanData([e],!0):st.support.deleteExpando||u!=u.window?delete u[l]:u[l]=null)}}}function a(e,n,r){if(r===t&&1===e.nodeType){var i="data-"+n.replace(Nt,"-$1").toLowerCase();if(r=e.getAttribute(i),"string"==typeof r){try{r="true"===r?!0:"false"===r?!1:"null"===r?null:+r+""===r?+r:wt.test(r)?st.parseJSON(r):r}catch(o){}st.data(e,n,r)}else r=t}return r}function s(e){var t;for(t in e)if(("data"!==t||!st.isEmptyObject(e[t]))&&"toJSON"!==t)return!1;return!0}function u(){return!0}function l(){return!1}function c(e,t){do e=e[t];while(e&&1!==e.nodeType);return e}function f(e,t,n){if(t=t||0,st.isFunction(t))return st.grep(e,function(e,r){var i=!!t.call(e,r,e);return i===n});if(t.nodeType)return st.grep(e,function(e){return e===t===n});if("string"==typeof t){var r=st.grep(e,function(e){return 1===e.nodeType});if(Wt.test(t))return st.filter(t,r,!n);t=st.filter(t,r)}return st.grep(e,function(e){return st.inArray(e,t)>=0===n})}function p(e){var t=zt.split("|"),n=e.createDocumentFragment();if(n.createElement)for(;t.length;)n.createElement(t.pop());return n}function d(e,t){return e.getElementsByTagName(t)[0]||e.appendChild(e.ownerDocument.createElement(t))}function h(e){var t=e.getAttributeNode("type");return e.type=(t&&t.specified)+"/"+e.type,e}function g(e){var t=nn.exec(e.type);return t?e.type=t[1]:e.removeAttribute("type"),e}function m(e,t){for(var n,r=0;null!=(n=e[r]);r++)st._data(n,"globalEval",!t||st._data(t[r],"globalEval"))}function y(e,t){if(1===t.nodeType&&st.hasData(e)){var n,r,i,o=st._data(e),a=st._data(t,o),s=o.events;if(s){delete a.handle,a.events={};for(n in s)for(r=0,i=s[n].length;i>r;r++)st.event.add(t,n,s[n][r])}a.data&&(a.data=st.extend({},a.data))}}function v(e,t){var n,r,i;if(1===t.nodeType){if(n=t.nodeName.toLowerCase(),!st.support.noCloneEvent&&t[st.expando]){r=st._data(t);for(i in r.events)st.removeEvent(t,i,r.handle);t.removeAttribute(st.expando)}"script"===n&&t.text!==e.text?(h(t).text=e.text,g(t)):"object"===n?(t.parentNode&&(t.outerHTML=e.outerHTML),st.support.html5Clone&&e.innerHTML&&!st.trim(t.innerHTML)&&(t.innerHTML=e.innerHTML)):"input"===n&&Zt.test(e.type)?(t.defaultChecked=t.checked=e.checked,t.value!==e.value&&(t.value=e.value)):"option"===n?t.defaultSelected=t.selected=e.defaultSelected:("input"===n||"textarea"===n)&&(t.defaultValue=e.defaultValue)}}function b(e,n){var r,i,o=0,a=e.getElementsByTagName!==t?e.getElementsByTagName(n||"*"):e.querySelectorAll!==t?e.querySelectorAll(n||"*"):t;if(!a)for(a=[],r=e.childNodes||e;null!=(i=r[o]);o++)!n||st.nodeName(i,n)?a.push(i):st.merge(a,b(i,n));return n===t||n&&st.nodeName(e,n)?st.merge([e],a):a}function x(e){Zt.test(e.type)&&(e.defaultChecked=e.checked)}function T(e,t){if(t in e)return t;for(var n=t.charAt(0).toUpperCase()+t.slice(1),r=t,i=Nn.length;i--;)if(t=Nn[i]+n,t in e)return t;return r}function w(e,t){return e=t||e,"none"===st.css(e,"display")||!st.contains(e.ownerDocument,e)}function N(e,t){for(var n,r=[],i=0,o=e.length;o>i;i++)n=e[i],n.style&&(r[i]=st._data(n,"olddisplay"),t?(r[i]||"none"!==n.style.display||(n.style.display=""),""===n.style.display&&w(n)&&(r[i]=st._data(n,"olddisplay",S(n.nodeName)))):r[i]||w(n)||st._data(n,"olddisplay",st.css(n,"display")));for(i=0;o>i;i++)n=e[i],n.style&&(t&&"none"!==n.style.display&&""!==n.style.display||(n.style.display=t?r[i]||"":"none"));return e}function C(e,t,n){var r=mn.exec(t);return r?Math.max(0,r[1]-(n||0))+(r[2]||"px"):t}function k(e,t,n,r,i){for(var o=n===(r?"border":"content")?4:"width"===t?1:0,a=0;4>o;o+=2)"margin"===n&&(a+=st.css(e,n+wn[o],!0,i)),r?("content"===n&&(a-=st.css(e,"padding"+wn[o],!0,i)),"margin"!==n&&(a-=st.css(e,"border"+wn[o]+"Width",!0,i))):(a+=st.css(e,"padding"+wn[o],!0,i),"padding"!==n&&(a+=st.css(e,"border"+wn[o]+"Width",!0,i)));return a}function E(e,t,n){var r=!0,i="width"===t?e.offsetWidth:e.offsetHeight,o=ln(e),a=st.support.boxSizing&&"border-box"===st.css(e,"boxSizing",!1,o);if(0>=i||null==i){if(i=un(e,t,o),(0>i||null==i)&&(i=e.style[t]),yn.test(i))return i;r=a&&(st.support.boxSizingReliable||i===e.style[t]),i=parseFloat(i)||0}return i+k(e,t,n||(a?"border":"content"),r,o)+"px"}function S(e){var t=V,n=bn[e];return n||(n=A(e,t),"none"!==n&&n||(cn=(cn||st("<iframe frameborder='0' width='0' height='0'/>").css("cssText","display:block !important")).appendTo(t.documentElement),t=(cn[0].contentWindow||cn[0].contentDocument).document,t.write("<!doctype html><html><body>"),t.close(),n=A(e,t),cn.detach()),bn[e]=n),n}function A(e,t){var n=st(t.createElement(e)).appendTo(t.body),r=st.css(n[0],"display");return n.remove(),r}function j(e,t,n,r){var i;if(st.isArray(t))st.each(t,function(t,i){n||kn.test(e)?r(e,i):j(e+"["+("object"==typeof i?t:"")+"]",i,n,r)});else if(n||"object"!==st.type(t))r(e,t);else for(i in t)j(e+"["+i+"]",t[i],n,r)}function D(e){return function(t,n){"string"!=typeof t&&(n=t,t="*");var r,i=0,o=t.toLowerCase().match(lt)||[];if(st.isFunction(n))for(;r=o[i++];)"+"===r[0]?(r=r.slice(1)||"*",(e[r]=e[r]||[]).unshift(n)):(e[r]=e[r]||[]).push(n)}}function L(e,n,r,i){function o(u){var l;return a[u]=!0,st.each(e[u]||[],function(e,u){var c=u(n,r,i);return"string"!=typeof c||s||a[c]?s?!(l=c):t:(n.dataTypes.unshift(c),o(c),!1)}),l}var a={},s=e===$n;return o(n.dataTypes[0])||!a["*"]&&o("*")}function H(e,n){var r,i,o=st.ajaxSettings.flatOptions||{};for(r in n)n[r]!==t&&((o[r]?e:i||(i={}))[r]=n[r]);return i&&st.extend(!0,e,i),e}function M(e,n,r){var i,o,a,s,u=e.contents,l=e.dataTypes,c=e.responseFields;for(o in c)o in r&&(n[c[o]]=r[o]);for(;"*"===l[0];)l.shift(),i===t&&(i=e.mimeType||n.getResponseHeader("Content-Type"));if(i)for(o in u)if(u[o]&&u[o].test(i)){l.unshift(o);break}if(l[0]in r)a=l[0];else{for(o in r){if(!l[0]||e.converters[o+" "+l[0]]){a=o;break}s||(s=o)}a=a||s}return a?(a!==l[0]&&l.unshift(a),r[a]):t}function q(e,t){var n,r,i,o,a={},s=0,u=e.dataTypes.slice(),l=u[0];if(e.dataFilter&&(t=e.dataFilter(t,e.dataType)),u[1])for(n in e.converters)a[n.toLowerCase()]=e.converters[n];for(;i=u[++s];)if("*"!==i){if("*"!==l&&l!==i){if(n=a[l+" "+i]||a["* "+i],!n)for(r in a)if(o=r.split(" "),o[1]===i&&(n=a[l+" "+o[0]]||a["* "+o[0]])){n===!0?n=a[r]:a[r]!==!0&&(i=o[0],u.splice(s--,0,i));break}if(n!==!0)if(n&&e["throws"])t=n(t);else try{t=n(t)}catch(c){return{state:"parsererror",error:n?c:"No conversion from "+l+" to "+i}}}l=i}return{state:"success",data:t}}function _(){try{return new e.XMLHttpRequest}catch(t){}}function F(){try{return new e.ActiveXObject("Microsoft.XMLHTTP")}catch(t){}}function O(){return setTimeout(function(){Qn=t}),Qn=st.now()}function B(e,t){st.each(t,function(t,n){for(var r=(rr[t]||[]).concat(rr["*"]),i=0,o=r.length;o>i;i++)if(r[i].call(e,t,n))return})}function P(e,t,n){var r,i,o=0,a=nr.length,s=st.Deferred().always(function(){delete u.elem}),u=function(){if(i)return!1;for(var t=Qn||O(),n=Math.max(0,l.startTime+l.duration-t),r=n/l.duration||0,o=1-r,a=0,u=l.tweens.length;u>a;a++)l.tweens[a].run(o);return s.notifyWith(e,[l,o,n]),1>o&&u?n:(s.resolveWith(e,[l]),!1)},l=s.promise({elem:e,props:st.extend({},t),opts:st.extend(!0,{specialEasing:{}},n),originalProperties:t,originalOptions:n,startTime:Qn||O(),duration:n.duration,tweens:[],createTween:function(t,n){var r=st.Tween(e,l.opts,t,n,l.opts.specialEasing[t]||l.opts.easing);return l.tweens.push(r),r},stop:function(t){var n=0,r=t?l.tweens.length:0;if(i)return this;for(i=!0;r>n;n++)l.tweens[n].run(1);return t?s.resolveWith(e,[l,t]):s.rejectWith(e,[l,t]),this}}),c=l.props;for(R(c,l.opts.specialEasing);a>o;o++)if(r=nr[o].call(l,e,c,l.opts))return r;return B(l,c),st.isFunction(l.opts.start)&&l.opts.start.call(e,l),st.fx.timer(st.extend(u,{elem:e,anim:l,queue:l.opts.queue})),l.progress(l.opts.progress).done(l.opts.done,l.opts.complete).fail(l.opts.fail).always(l.opts.always)}function R(e,t){var n,r,i,o,a;for(n in e)if(r=st.camelCase(n),i=t[r],o=e[n],st.isArray(o)&&(i=o[1],o=e[n]=o[0]),n!==r&&(e[r]=o,delete e[n]),a=st.cssHooks[r],a&&"expand"in a){o=a.expand(o),delete e[r];for(n in o)n in e||(e[n]=o[n],t[n]=i)}else t[r]=i}function W(e,t,n){var r,i,o,a,s,u,l,c,f,p=this,d=e.style,h={},g=[],m=e.nodeType&&w(e);n.queue||(c=st._queueHooks(e,"fx"),null==c.unqueued&&(c.unqueued=0,f=c.empty.fire,c.empty.fire=function(){c.unqueued||f()}),c.unqueued++,p.always(function(){p.always(function(){c.unqueued--,st.queue(e,"fx").length||c.empty.fire()})})),1===e.nodeType&&("height"in t||"width"in t)&&(n.overflow=[d.overflow,d.overflowX,d.overflowY],"inline"===st.css(e,"display")&&"none"===st.css(e,"float")&&(st.support.inlineBlockNeedsLayout&&"inline"!==S(e.nodeName)?d.zoom=1:d.display="inline-block")),n.overflow&&(d.overflow="hidden",st.support.shrinkWrapBlocks||p.done(function(){d.overflow=n.overflow[0],d.overflowX=n.overflow[1],d.overflowY=n.overflow[2]}));for(r in t)if(o=t[r],Zn.exec(o)){if(delete t[r],u=u||"toggle"===o,o===(m?"hide":"show"))continue;g.push(r)}if(a=g.length){s=st._data(e,"fxshow")||st._data(e,"fxshow",{}),"hidden"in s&&(m=s.hidden),u&&(s.hidden=!m),m?st(e).show():p.done(function(){st(e).hide()}),p.done(function(){var t;st._removeData(e,"fxshow");for(t in h)st.style(e,t,h[t])});for(r=0;a>r;r++)i=g[r],l=p.createTween(i,m?s[i]:0),h[i]=s[i]||st.style(e,i),i in s||(s[i]=l.start,m&&(l.end=l.start,l.start="width"===i||"height"===i?1:0))}}function $(e,t,n,r,i){return new $.prototype.init(e,t,n,r,i)}function I(e,t){var n,r={height:e},i=0;for(t=t?1:0;4>i;i+=2-t)n=wn[i],r["margin"+n]=r["padding"+n]=e;return t&&(r.opacity=r.width=e),r}function z(e){return st.isWindow(e)?e:9===e.nodeType?e.defaultView||e.parentWindow:!1}var X,U,V=e.document,Y=e.location,J=e.jQuery,G=e.$,Q={},K=[],Z="1.9.0",et=K.concat,tt=K.push,nt=K.slice,rt=K.indexOf,it=Q.toString,ot=Q.hasOwnProperty,at=Z.trim,st=function(e,t){return new st.fn.init(e,t,X)},ut=/[+-]?(?:\d*\.|)\d+(?:[eE][+-]?\d+|)/.source,lt=/\S+/g,ct=/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g,ft=/^(?:(<[\w\W]+>)[^>]*|#([\w-]*))$/,pt=/^<(\w+)\s*\/?>(?:<\/\1>|)$/,dt=/^[\],:{}\s]*$/,ht=/(?:^|:|,)(?:\s*\[)+/g,gt=/\\(?:["\\\/bfnrt]|u[\da-fA-F]{4})/g,mt=/"[^"\\\r\n]*"|true|false|null|-?(?:\d+\.|)\d+(?:[eE][+-]?\d+|)/g,yt=/^-ms-/,vt=/-([\da-z])/gi,bt=function(e,t){return t.toUpperCase()},xt=function(){V.addEventListener?(V.removeEventListener("DOMContentLoaded",xt,!1),st.ready()):"complete"===V.readyState&&(V.detachEvent("onreadystatechange",xt),st.ready())};st.fn=st.prototype={jquery:Z,constructor:st,init:function(e,n,r){var i,o;if(!e)return this;if("string"==typeof e){if(i="<"===e.charAt(0)&&">"===e.charAt(e.length-1)&&e.length>=3?[null,e,null]:ft.exec(e),!i||!i[1]&&n)return!n||n.jquery?(n||r).find(e):this.constructor(n).find(e);if(i[1]){if(n=n instanceof st?n[0]:n,st.merge(this,st.parseHTML(i[1],n&&n.nodeType?n.ownerDocument||n:V,!0)),pt.test(i[1])&&st.isPlainObject(n))for(i in n)st.isFunction(this[i])?this[i](n[i]):this.attr(i,n[i]);return this}if(o=V.getElementById(i[2]),o&&o.parentNode){if(o.id!==i[2])return r.find(e);this.length=1,this[0]=o}return this.context=V,this.selector=e,this}return e.nodeType?(this.context=this[0]=e,this.length=1,this):st.isFunction(e)?r.ready(e):(e.selector!==t&&(this.selector=e.selector,this.context=e.context),st.makeArray(e,this))},selector:"",length:0,size:function(){return this.length},toArray:function(){return nt.call(this)},get:function(e){return null==e?this.toArray():0>e?this[this.length+e]:this[e]},pushStack:function(e){var t=st.merge(this.constructor(),e);return t.prevObject=this,t.context=this.context,t},each:function(e,t){return st.each(this,e,t)},ready:function(e){return st.ready.promise().done(e),this},slice:function(){return this.pushStack(nt.apply(this,arguments))},first:function(){return this.eq(0)},last:function(){return this.eq(-1)},eq:function(e){var t=this.length,n=+e+(0>e?t:0);return this.pushStack(n>=0&&t>n?[this[n]]:[])},map:function(e){return this.pushStack(st.map(this,function(t,n){return e.call(t,n,t)}))},end:function(){return this.prevObject||this.constructor(null)},push:tt,sort:[].sort,splice:[].splice},st.fn.init.prototype=st.fn,st.extend=st.fn.extend=function(){var e,n,r,i,o,a,s=arguments[0]||{},u=1,l=arguments.length,c=!1;for("boolean"==typeof s&&(c=s,s=arguments[1]||{},u=2),"object"==typeof s||st.isFunction(s)||(s={}),l===u&&(s=this,--u);l>u;u++)if(null!=(e=arguments[u]))for(n in e)r=s[n],i=e[n],s!==i&&(c&&i&&(st.isPlainObject(i)||(o=st.isArray(i)))?(o?(o=!1,a=r&&st.isArray(r)?r:[]):a=r&&st.isPlainObject(r)?r:{},s[n]=st.extend(c,a,i)):i!==t&&(s[n]=i));return s},st.extend({noConflict:function(t){return e.$===st&&(e.$=G),t&&e.jQuery===st&&(e.jQuery=J),st},isReady:!1,readyWait:1,holdReady:function(e){e?st.readyWait++:st.ready(!0)},ready:function(e){if(e===!0?!--st.readyWait:!st.isReady){if(!V.body)return setTimeout(st.ready);st.isReady=!0,e!==!0&&--st.readyWait>0||(U.resolveWith(V,[st]),st.fn.trigger&&st(V).trigger("ready").off("ready"))}},isFunction:function(e){return"function"===st.type(e)},isArray:Array.isArray||function(e){return"array"===st.type(e)},isWindow:function(e){return null!=e&&e==e.window},isNumeric:function(e){return!isNaN(parseFloat(e))&&isFinite(e)},type:function(e){return null==e?e+"":"object"==typeof e||"function"==typeof e?Q[it.call(e)]||"object":typeof e},isPlainObject:function(e){if(!e||"object"!==st.type(e)||e.nodeType||st.isWindow(e))return!1;try{if(e.constructor&&!ot.call(e,"constructor")&&!ot.call(e.constructor.prototype,"isPrototypeOf"))return!1}catch(n){return!1}var r;for(r in e);return r===t||ot.call(e,r)},isEmptyObject:function(e){var t;for(t in e)return!1;return!0},error:function(e){throw Error(e)},parseHTML:function(e,t,n){if(!e||"string"!=typeof e)return null;"boolean"==typeof t&&(n=t,t=!1),t=t||V;var r=pt.exec(e),i=!n&&[];return r?[t.createElement(r[1])]:(r=st.buildFragment([e],t,i),i&&st(i).remove(),st.merge([],r.childNodes))},parseJSON:function(n){return e.JSON&&e.JSON.parse?e.JSON.parse(n):null===n?n:"string"==typeof n&&(n=st.trim(n),n&&dt.test(n.replace(gt,"@").replace(mt,"]").replace(ht,"")))?Function("return "+n)():(st.error("Invalid JSON: "+n),t)},parseXML:function(n){var r,i;if(!n||"string"!=typeof n)return null;try{e.DOMParser?(i=new DOMParser,r=i.parseFromString(n,"text/xml")):(r=new ActiveXObject("Microsoft.XMLDOM"),r.async="false",r.loadXML(n))}catch(o){r=t}return r&&r.documentElement&&!r.getElementsByTagName("parsererror").length||st.error("Invalid XML: "+n),r},noop:function(){},globalEval:function(t){t&&st.trim(t)&&(e.execScript||function(t){e.eval.call(e,t)})(t)},camelCase:function(e){return e.replace(yt,"ms-").replace(vt,bt)},nodeName:function(e,t){return e.nodeName&&e.nodeName.toLowerCase()===t.toLowerCase()},each:function(e,t,r){var i,o=0,a=e.length,s=n(e);if(r){if(s)for(;a>o&&(i=t.apply(e[o],r),i!==!1);o++);else for(o in e)if(i=t.apply(e[o],r),i===!1)break}else if(s)for(;a>o&&(i=t.call(e[o],o,e[o]),i!==!1);o++);else for(o in e)if(i=t.call(e[o],o,e[o]),i===!1)break;return e},trim:at&&!at.call("\ufeff\u00a0")?function(e){return null==e?"":at.call(e)}:function(e){return null==e?"":(e+"").replace(ct,"")},makeArray:function(e,t){var r=t||[];return null!=e&&(n(Object(e))?st.merge(r,"string"==typeof e?[e]:e):tt.call(r,e)),r},inArray:function(e,t,n){var r;if(t){if(rt)return rt.call(t,e,n);for(r=t.length,n=n?0>n?Math.max(0,r+n):n:0;r>n;n++)if(n in t&&t[n]===e)return n}return-1},merge:function(e,n){var r=n.length,i=e.length,o=0;if("number"==typeof r)for(;r>o;o++)e[i++]=n[o];else for(;n[o]!==t;)e[i++]=n[o++];return e.length=i,e},grep:function(e,t,n){var r,i=[],o=0,a=e.length;for(n=!!n;a>o;o++)r=!!t(e[o],o),n!==r&&i.push(e[o]);return i},map:function(e,t,r){var i,o=0,a=e.length,s=n(e),u=[];if(s)for(;a>o;o++)i=t(e[o],o,r),null!=i&&(u[u.length]=i);else for(o in e)i=t(e[o],o,r),null!=i&&(u[u.length]=i);return et.apply([],u)},guid:1,proxy:function(e,n){var r,i,o;return"string"==typeof n&&(r=e[n],n=e,e=r),st.isFunction(e)?(i=nt.call(arguments,2),o=function(){return e.apply(n||this,i.concat(nt.call(arguments)))},o.guid=e.guid=e.guid||st.guid++,o):t},access:function(e,n,r,i,o,a,s){var u=0,l=e.length,c=null==r;if("object"===st.type(r)){o=!0;for(u in r)st.access(e,n,u,r[u],!0,a,s)}else if(i!==t&&(o=!0,st.isFunction(i)||(s=!0),c&&(s?(n.call(e,i),n=null):(c=n,n=function(e,t,n){return c.call(st(e),n)})),n))for(;l>u;u++)n(e[u],r,s?i:i.call(e[u],u,n(e[u],r)));return o?e:c?n.call(e):l?n(e[0],r):a},now:function(){return(new Date).getTime()}}),st.ready.promise=function(t){if(!U)if(U=st.Deferred(),"complete"===V.readyState)setTimeout(st.ready);else if(V.addEventListener)V.addEventListener("DOMContentLoaded",xt,!1),e.addEventListener("load",st.ready,!1);else{V.attachEvent("onreadystatechange",xt),e.attachEvent("onload",st.ready);var n=!1;try{n=null==e.frameElement&&V.documentElement}catch(r){}n&&n.doScroll&&function i(){if(!st.isReady){try{n.doScroll("left")}catch(e){return setTimeout(i,50)}st.ready()}}()}return U.promise(t)},st.each("Boolean Number String Function Array Date RegExp Object Error".split(" "),function(e,t){Q["[object "+t+"]"]=t.toLowerCase()}),X=st(V);var Tt={};st.Callbacks=function(e){e="string"==typeof e?Tt[e]||r(e):st.extend({},e);var n,i,o,a,s,u,l=[],c=!e.once&&[],f=function(t){for(n=e.memory&&t,i=!0,u=a||0,a=0,s=l.length,o=!0;l&&s>u;u++)if(l[u].apply(t[0],t[1])===!1&&e.stopOnFalse){n=!1;break}o=!1,l&&(c?c.length&&f(c.shift()):n?l=[]:p.disable())},p={add:function(){if(l){var t=l.length;(function r(t){st.each(t,function(t,n){var i=st.type(n);"function"===i?e.unique&&p.has(n)||l.push(n):n&&n.length&&"string"!==i&&r(n)})})(arguments),o?s=l.length:n&&(a=t,f(n))}return this},remove:function(){return l&&st.each(arguments,function(e,t){for(var n;(n=st.inArray(t,l,n))>-1;)l.splice(n,1),o&&(s>=n&&s--,u>=n&&u--)}),this},has:function(e){return st.inArray(e,l)>-1},empty:function(){return l=[],this},disable:function(){return l=c=n=t,this},disabled:function(){return!l},lock:function(){return c=t,n||p.disable(),this},locked:function(){return!c},fireWith:function(e,t){return t=t||[],t=[e,t.slice?t.slice():t],!l||i&&!c||(o?c.push(t):f(t)),this},fire:function(){return p.fireWith(this,arguments),this},fired:function(){return!!i}};return p},st.extend({Deferred:function(e){var t=[["resolve","done",st.Callbacks("once memory"),"resolved"],["reject","fail",st.Callbacks("once memory"),"rejected"],["notify","progress",st.Callbacks("memory")]],n="pending",r={state:function(){return n},always:function(){return i.done(arguments).fail(arguments),this},then:function(){var e=arguments;return st.Deferred(function(n){st.each(t,function(t,o){var a=o[0],s=st.isFunction(e[t])&&e[t];i[o[1]](function(){var e=s&&s.apply(this,arguments);e&&st.isFunction(e.promise)?e.promise().done(n.resolve).fail(n.reject).progress(n.notify):n[a+"With"](this===r?n.promise():this,s?[e]:arguments)})}),e=null}).promise()},promise:function(e){return null!=e?st.extend(e,r):r}},i={};return r.pipe=r.then,st.each(t,function(e,o){var a=o[2],s=o[3];r[o[1]]=a.add,s&&a.add(function(){n=s},t[1^e][2].disable,t[2][2].lock),i[o[0]]=function(){return i[o[0]+"With"](this===i?r:this,arguments),this},i[o[0]+"With"]=a.fireWith}),r.promise(i),e&&e.call(i,i),i},when:function(e){var t,n,r,i=0,o=nt.call(arguments),a=o.length,s=1!==a||e&&st.isFunction(e.promise)?a:0,u=1===s?e:st.Deferred(),l=function(e,n,r){return function(i){n[e]=this,r[e]=arguments.length>1?nt.call(arguments):i,r===t?u.notifyWith(n,r):--s||u.resolveWith(n,r)}};if(a>1)for(t=Array(a),n=Array(a),r=Array(a);a>i;i++)o[i]&&st.isFunction(o[i].promise)?o[i].promise().done(l(i,r,o)).fail(u.reject).progress(l(i,n,t)):--s;return s||u.resolveWith(r,o),u.promise()}}),st.support=function(){var n,r,i,o,a,s,u,l,c,f,p=V.createElement("div");if(p.setAttribute("className","t"),p.innerHTML="  <link/><table></table><a href='/a'>a</a><input type='checkbox'/>",r=p.getElementsByTagName("*"),i=p.getElementsByTagName("a")[0],!r||!i||!r.length)return{};o=V.createElement("select"),a=o.appendChild(V.createElement("option")),s=p.getElementsByTagName("input")[0],i.style.cssText="top:1px;float:left;opacity:.5",n={getSetAttribute:"t"!==p.className,leadingWhitespace:3===p.firstChild.nodeType,tbody:!p.getElementsByTagName("tbody").length,htmlSerialize:!!p.getElementsByTagName("link").length,style:/top/.test(i.getAttribute("style")),hrefNormalized:"/a"===i.getAttribute("href"),opacity:/^0.5/.test(i.style.opacity),cssFloat:!!i.style.cssFloat,checkOn:!!s.value,optSelected:a.selected,enctype:!!V.createElement("form").enctype,html5Clone:"<:nav></:nav>"!==V.createElement("nav").cloneNode(!0).outerHTML,boxModel:"CSS1Compat"===V.compatMode,deleteExpando:!0,noCloneEvent:!0,inlineBlockNeedsLayout:!1,shrinkWrapBlocks:!1,reliableMarginRight:!0,boxSizingReliable:!0,pixelPosition:!1},s.checked=!0,n.noCloneChecked=s.cloneNode(!0).checked,o.disabled=!0,n.optDisabled=!a.disabled;try{delete p.test}catch(d){n.deleteExpando=!1}s=V.createElement("input"),s.setAttribute("value",""),n.input=""===s.getAttribute("value"),s.value="t",s.setAttribute("type","radio"),n.radioValue="t"===s.value,s.setAttribute("checked","t"),s.setAttribute("name","t"),u=V.createDocumentFragment(),u.appendChild(s),n.appendChecked=s.checked,n.checkClone=u.cloneNode(!0).cloneNode(!0).lastChild.checked,p.attachEvent&&(p.attachEvent("onclick",function(){n.noCloneEvent=!1}),p.cloneNode(!0).click());for(f in{submit:!0,change:!0,focusin:!0})p.setAttribute(l="on"+f,"t"),n[f+"Bubbles"]=l in e||p.attributes[l].expando===!1;return p.style.backgroundClip="content-box",p.cloneNode(!0).style.backgroundClip="",n.clearCloneStyle="content-box"===p.style.backgroundClip,st(function(){var r,i,o,a="padding:0;margin:0;border:0;display:block;box-sizing:content-box;-moz-box-sizing:content-box;-webkit-box-sizing:content-box;",s=V.getElementsByTagName("body")[0];s&&(r=V.createElement("div"),r.style.cssText="border:0;width:0;height:0;position:absolute;top:0;left:-9999px;margin-top:1px",s.appendChild(r).appendChild(p),p.innerHTML="<table><tr><td></td><td>t</td></tr></table>",o=p.getElementsByTagName("td"),o[0].style.cssText="padding:0;margin:0;border:0;display:none",c=0===o[0].offsetHeight,o[0].style.display="",o[1].style.display="none",n.reliableHiddenOffsets=c&&0===o[0].offsetHeight,p.innerHTML="",p.style.cssText="box-sizing:border-box;-moz-box-sizing:border-box;-webkit-box-sizing:border-box;padding:1px;border:1px;display:block;width:4px;margin-top:1%;position:absolute;top:1%;",n.boxSizing=4===p.offsetWidth,n.doesNotIncludeMarginInBodyOffset=1!==s.offsetTop,e.getComputedStyle&&(n.pixelPosition="1%"!==(e.getComputedStyle(p,null)||{}).top,n.boxSizingReliable="4px"===(e.getComputedStyle(p,null)||{width:"4px"}).width,i=p.appendChild(V.createElement("div")),i.style.cssText=p.style.cssText=a,i.style.marginRight=i.style.width="0",p.style.width="1px",n.reliableMarginRight=!parseFloat((e.getComputedStyle(i,null)||{}).marginRight)),p.style.zoom!==t&&(p.innerHTML="",p.style.cssText=a+"width:1px;padding:1px;display:inline;zoom:1",n.inlineBlockNeedsLayout=3===p.offsetWidth,p.style.display="block",p.innerHTML="<div></div>",p.firstChild.style.width="5px",n.shrinkWrapBlocks=3!==p.offsetWidth,s.style.zoom=1),s.removeChild(r),r=p=o=i=null)}),r=o=u=a=i=s=null,n}();var wt=/(?:\{[\s\S]*\}|\[[\s\S]*\])$/,Nt=/([A-Z])/g;st.extend({cache:{},expando:"jQuery"+(Z+Math.random()).replace(/\D/g,""),noData:{embed:!0,object:"clsid:D27CDB6E-AE6D-11cf-96B8-444553540000",applet:!0},hasData:function(e){return e=e.nodeType?st.cache[e[st.expando]]:e[st.expando],!!e&&!s(e)},data:function(e,t,n){return i(e,t,n,!1)},removeData:function(e,t){return o(e,t,!1)},_data:function(e,t,n){return i(e,t,n,!0)},_removeData:function(e,t){return o(e,t,!0)},acceptData:function(e){var t=e.nodeName&&st.noData[e.nodeName.toLowerCase()];return!t||t!==!0&&e.getAttribute("classid")===t}}),st.fn.extend({data:function(e,n){var r,i,o=this[0],s=0,u=null;if(e===t){if(this.length&&(u=st.data(o),1===o.nodeType&&!st._data(o,"parsedAttrs"))){for(r=o.attributes;r.length>s;s++)i=r[s].name,i.indexOf("data-")||(i=st.camelCase(i.substring(5)),a(o,i,u[i]));st._data(o,"parsedAttrs",!0)}return u}return"object"==typeof e?this.each(function(){st.data(this,e)}):st.access(this,function(n){return n===t?o?a(o,e,st.data(o,e)):null:(this.each(function(){st.data(this,e,n)}),t)},null,n,arguments.length>1,null,!0)},removeData:function(e){return this.each(function(){st.removeData(this,e)})}}),st.extend({queue:function(e,n,r){var i;return e?(n=(n||"fx")+"queue",i=st._data(e,n),r&&(!i||st.isArray(r)?i=st._data(e,n,st.makeArray(r)):i.push(r)),i||[]):t},dequeue:function(e,t){t=t||"fx";var n=st.queue(e,t),r=n.length,i=n.shift(),o=st._queueHooks(e,t),a=function(){st.dequeue(e,t)};"inprogress"===i&&(i=n.shift(),r--),o.cur=i,i&&("fx"===t&&n.unshift("inprogress"),delete o.stop,i.call(e,a,o)),!r&&o&&o.empty.fire()},_queueHooks:function(e,t){var n=t+"queueHooks";return st._data(e,n)||st._data(e,n,{empty:st.Callbacks("once memory").add(function(){st._removeData(e,t+"queue"),st._removeData(e,n)})})}}),st.fn.extend({queue:function(e,n){var r=2;return"string"!=typeof e&&(n=e,e="fx",r--),r>arguments.length?st.queue(this[0],e):n===t?this:this.each(function(){var t=st.queue(this,e,n);st._queueHooks(this,e),"fx"===e&&"inprogress"!==t[0]&&st.dequeue(this,e)})},dequeue:function(e){return this.each(function(){st.dequeue(this,e)})},delay:function(e,t){return e=st.fx?st.fx.speeds[e]||e:e,t=t||"fx",this.queue(t,function(t,n){var r=setTimeout(t,e);n.stop=function(){clearTimeout(r)}})},clearQueue:function(e){return this.queue(e||"fx",[])},promise:function(e,n){var r,i=1,o=st.Deferred(),a=this,s=this.length,u=function(){--i||o.resolveWith(a,[a])};for("string"!=typeof e&&(n=e,e=t),e=e||"fx";s--;)r=st._data(a[s],e+"queueHooks"),r&&r.empty&&(i++,r.empty.add(u));return u(),o.promise(n)}});var Ct,kt,Et=/[\t\r\n]/g,St=/\r/g,At=/^(?:input|select|textarea|button|object)$/i,jt=/^(?:a|area)$/i,Dt=/^(?:checked|selected|autofocus|autoplay|async|controls|defer|disabled|hidden|loop|multiple|open|readonly|required|scoped)$/i,Lt=/^(?:checked|selected)$/i,Ht=st.support.getSetAttribute,Mt=st.support.input;st.fn.extend({attr:function(e,t){return st.access(this,st.attr,e,t,arguments.length>1)},removeAttr:function(e){return this.each(function(){st.removeAttr(this,e)})},prop:function(e,t){return st.access(this,st.prop,e,t,arguments.length>1)},removeProp:function(e){return e=st.propFix[e]||e,this.each(function(){try{this[e]=t,delete this[e]}catch(n){}})},addClass:function(e){var t,n,r,i,o,a=0,s=this.length,u="string"==typeof e&&e;if(st.isFunction(e))return this.each(function(t){st(this).addClass(e.call(this,t,this.className))});if(u)for(t=(e||"").match(lt)||[];s>a;a++)if(n=this[a],r=1===n.nodeType&&(n.className?(" "+n.className+" ").replace(Et," "):" ")){for(o=0;i=t[o++];)0>r.indexOf(" "+i+" ")&&(r+=i+" ");n.className=st.trim(r)}return this},removeClass:function(e){var t,n,r,i,o,a=0,s=this.length,u=0===arguments.length||"string"==typeof e&&e;if(st.isFunction(e))return this.each(function(t){st(this).removeClass(e.call(this,t,this.className))});if(u)for(t=(e||"").match(lt)||[];s>a;a++)if(n=this[a],r=1===n.nodeType&&(n.className?(" "+n.className+" ").replace(Et," "):"")){for(o=0;i=t[o++];)for(;r.indexOf(" "+i+" ")>=0;)r=r.replace(" "+i+" "," ");n.className=e?st.trim(r):""}return this},toggleClass:function(e,t){var n=typeof e,r="boolean"==typeof t;return st.isFunction(e)?this.each(function(n){st(this).toggleClass(e.call(this,n,this.className,t),t)}):this.each(function(){if("string"===n)for(var i,o=0,a=st(this),s=t,u=e.match(lt)||[];i=u[o++];)s=r?s:!a.hasClass(i),a[s?"addClass":"removeClass"](i);else("undefined"===n||"boolean"===n)&&(this.className&&st._data(this,"__className__",this.className),this.className=this.className||e===!1?"":st._data(this,"__className__")||"")})},hasClass:function(e){for(var t=" "+e+" ",n=0,r=this.length;r>n;n++)if(1===this[n].nodeType&&(" "+this[n].className+" ").replace(Et," ").indexOf(t)>=0)return!0;return!1},val:function(e){var n,r,i,o=this[0];{if(arguments.length)return i=st.isFunction(e),this.each(function(r){var o,a=st(this);1===this.nodeType&&(o=i?e.call(this,r,a.val()):e,null==o?o="":"number"==typeof o?o+="":st.isArray(o)&&(o=st.map(o,function(e){return null==e?"":e+""})),n=st.valHooks[this.type]||st.valHooks[this.nodeName.toLowerCase()],n&&"set"in n&&n.set(this,o,"value")!==t||(this.value=o))});if(o)return n=st.valHooks[o.type]||st.valHooks[o.nodeName.toLowerCase()],n&&"get"in n&&(r=n.get(o,"value"))!==t?r:(r=o.value,"string"==typeof r?r.replace(St,""):null==r?"":r)}}}),st.extend({valHooks:{option:{get:function(e){var t=e.attributes.value;return!t||t.specified?e.value:e.text}},select:{get:function(e){for(var t,n,r=e.options,i=e.selectedIndex,o="select-one"===e.type||0>i,a=o?null:[],s=o?i+1:r.length,u=0>i?s:o?i:0;s>u;u++)if(n=r[u],!(!n.selected&&u!==i||(st.support.optDisabled?n.disabled:null!==n.getAttribute("disabled"))||n.parentNode.disabled&&st.nodeName(n.parentNode,"optgroup"))){if(t=st(n).val(),o)return t;a.push(t)}return a},set:function(e,t){var n=st.makeArray(t);return st(e).find("option").each(function(){this.selected=st.inArray(st(this).val(),n)>=0}),n.length||(e.selectedIndex=-1),n}}},attr:function(e,n,r){var i,o,a,s=e.nodeType;if(e&&3!==s&&8!==s&&2!==s)return e.getAttribute===t?st.prop(e,n,r):(a=1!==s||!st.isXMLDoc(e),a&&(n=n.toLowerCase(),o=st.attrHooks[n]||(Dt.test(n)?kt:Ct)),r===t?o&&a&&"get"in o&&null!==(i=o.get(e,n))?i:(e.getAttribute!==t&&(i=e.getAttribute(n)),null==i?t:i):null!==r?o&&a&&"set"in o&&(i=o.set(e,r,n))!==t?i:(e.setAttribute(n,r+""),r):(st.removeAttr(e,n),t))},removeAttr:function(e,t){var n,r,i=0,o=t&&t.match(lt);if(o&&1===e.nodeType)for(;n=o[i++];)r=st.propFix[n]||n,Dt.test(n)?!Ht&&Lt.test(n)?e[st.camelCase("default-"+n)]=e[r]=!1:e[r]=!1:st.attr(e,n,""),e.removeAttribute(Ht?n:r)},attrHooks:{type:{set:function(e,t){if(!st.support.radioValue&&"radio"===t&&st.nodeName(e,"input")){var n=e.value;return e.setAttribute("type",t),n&&(e.value=n),t}}}},propFix:{tabindex:"tabIndex",readonly:"readOnly","for":"htmlFor","class":"className",maxlength:"maxLength",cellspacing:"cellSpacing",cellpadding:"cellPadding",rowspan:"rowSpan",colspan:"colSpan",usemap:"useMap",frameborder:"frameBorder",contenteditable:"contentEditable"},prop:function(e,n,r){var i,o,a,s=e.nodeType;if(e&&3!==s&&8!==s&&2!==s)return a=1!==s||!st.isXMLDoc(e),a&&(n=st.propFix[n]||n,o=st.propHooks[n]),r!==t?o&&"set"in o&&(i=o.set(e,r,n))!==t?i:e[n]=r:o&&"get"in o&&null!==(i=o.get(e,n))?i:e[n]},propHooks:{tabIndex:{get:function(e){var n=e.getAttributeNode("tabindex");return n&&n.specified?parseInt(n.value,10):At.test(e.nodeName)||jt.test(e.nodeName)&&e.href?0:t}}}}),kt={get:function(e,n){var r=st.prop(e,n),i="boolean"==typeof r&&e.getAttribute(n),o="boolean"==typeof r?Mt&&Ht?null!=i:Lt.test(n)?e[st.camelCase("default-"+n)]:!!i:e.getAttributeNode(n);return o&&o.value!==!1?n.toLowerCase():t},set:function(e,t,n){return t===!1?st.removeAttr(e,n):Mt&&Ht||!Lt.test(n)?e.setAttribute(!Ht&&st.propFix[n]||n,n):e[st.camelCase("default-"+n)]=e[n]=!0,n}},Mt&&Ht||(st.attrHooks.value={get:function(e,n){var r=e.getAttributeNode(n);return st.nodeName(e,"input")?e.defaultValue:r&&r.specified?r.value:t
},set:function(e,n,r){return st.nodeName(e,"input")?(e.defaultValue=n,t):Ct&&Ct.set(e,n,r)}}),Ht||(Ct=st.valHooks.button={get:function(e,n){var r=e.getAttributeNode(n);return r&&("id"===n||"name"===n||"coords"===n?""!==r.value:r.specified)?r.value:t},set:function(e,n,r){var i=e.getAttributeNode(r);return i||e.setAttributeNode(i=e.ownerDocument.createAttribute(r)),i.value=n+="","value"===r||n===e.getAttribute(r)?n:t}},st.attrHooks.contenteditable={get:Ct.get,set:function(e,t,n){Ct.set(e,""===t?!1:t,n)}},st.each(["width","height"],function(e,n){st.attrHooks[n]=st.extend(st.attrHooks[n],{set:function(e,r){return""===r?(e.setAttribute(n,"auto"),r):t}})})),st.support.hrefNormalized||(st.each(["href","src","width","height"],function(e,n){st.attrHooks[n]=st.extend(st.attrHooks[n],{get:function(e){var r=e.getAttribute(n,2);return null==r?t:r}})}),st.each(["href","src"],function(e,t){st.propHooks[t]={get:function(e){return e.getAttribute(t,4)}}})),st.support.style||(st.attrHooks.style={get:function(e){return e.style.cssText||t},set:function(e,t){return e.style.cssText=t+""}}),st.support.optSelected||(st.propHooks.selected=st.extend(st.propHooks.selected,{get:function(e){var t=e.parentNode;return t&&(t.selectedIndex,t.parentNode&&t.parentNode.selectedIndex),null}})),st.support.enctype||(st.propFix.enctype="encoding"),st.support.checkOn||st.each(["radio","checkbox"],function(){st.valHooks[this]={get:function(e){return null===e.getAttribute("value")?"on":e.value}}}),st.each(["radio","checkbox"],function(){st.valHooks[this]=st.extend(st.valHooks[this],{set:function(e,n){return st.isArray(n)?e.checked=st.inArray(st(e).val(),n)>=0:t}})});var qt=/^(?:input|select|textarea)$/i,_t=/^key/,Ft=/^(?:mouse|contextmenu)|click/,Ot=/^(?:focusinfocus|focusoutblur)$/,Bt=/^([^.]*)(?:\.(.+)|)$/;st.event={global:{},add:function(e,n,r,i,o){var a,s,u,l,c,f,p,d,h,g,m,y=3!==e.nodeType&&8!==e.nodeType&&st._data(e);if(y){for(r.handler&&(a=r,r=a.handler,o=a.selector),r.guid||(r.guid=st.guid++),(l=y.events)||(l=y.events={}),(s=y.handle)||(s=y.handle=function(e){return st===t||e&&st.event.triggered===e.type?t:st.event.dispatch.apply(s.elem,arguments)},s.elem=e),n=(n||"").match(lt)||[""],c=n.length;c--;)u=Bt.exec(n[c])||[],h=m=u[1],g=(u[2]||"").split(".").sort(),p=st.event.special[h]||{},h=(o?p.delegateType:p.bindType)||h,p=st.event.special[h]||{},f=st.extend({type:h,origType:m,data:i,handler:r,guid:r.guid,selector:o,needsContext:o&&st.expr.match.needsContext.test(o),namespace:g.join(".")},a),(d=l[h])||(d=l[h]=[],d.delegateCount=0,p.setup&&p.setup.call(e,i,g,s)!==!1||(e.addEventListener?e.addEventListener(h,s,!1):e.attachEvent&&e.attachEvent("on"+h,s))),p.add&&(p.add.call(e,f),f.handler.guid||(f.handler.guid=r.guid)),o?d.splice(d.delegateCount++,0,f):d.push(f),st.event.global[h]=!0;e=null}},remove:function(e,t,n,r,i){var o,a,s,u,l,c,f,p,d,h,g,m=st.hasData(e)&&st._data(e);if(m&&(u=m.events)){for(t=(t||"").match(lt)||[""],l=t.length;l--;)if(s=Bt.exec(t[l])||[],d=g=s[1],h=(s[2]||"").split(".").sort(),d){for(f=st.event.special[d]||{},d=(r?f.delegateType:f.bindType)||d,p=u[d]||[],s=s[2]&&RegExp("(^|\\.)"+h.join("\\.(?:.*\\.|)")+"(\\.|$)"),a=o=p.length;o--;)c=p[o],!i&&g!==c.origType||n&&n.guid!==c.guid||s&&!s.test(c.namespace)||r&&r!==c.selector&&("**"!==r||!c.selector)||(p.splice(o,1),c.selector&&p.delegateCount--,f.remove&&f.remove.call(e,c));a&&!p.length&&(f.teardown&&f.teardown.call(e,h,m.handle)!==!1||st.removeEvent(e,d,m.handle),delete u[d])}else for(d in u)st.event.remove(e,d+t[l],n,r,!0);st.isEmptyObject(u)&&(delete m.handle,st._removeData(e,"events"))}},trigger:function(n,r,i,o){var a,s,u,l,c,f,p,d=[i||V],h=n.type||n,g=n.namespace?n.namespace.split("."):[];if(s=u=i=i||V,3!==i.nodeType&&8!==i.nodeType&&!Ot.test(h+st.event.triggered)&&(h.indexOf(".")>=0&&(g=h.split("."),h=g.shift(),g.sort()),c=0>h.indexOf(":")&&"on"+h,n=n[st.expando]?n:new st.Event(h,"object"==typeof n&&n),n.isTrigger=!0,n.namespace=g.join("."),n.namespace_re=n.namespace?RegExp("(^|\\.)"+g.join("\\.(?:.*\\.|)")+"(\\.|$)"):null,n.result=t,n.target||(n.target=i),r=null==r?[n]:st.makeArray(r,[n]),p=st.event.special[h]||{},o||!p.trigger||p.trigger.apply(i,r)!==!1)){if(!o&&!p.noBubble&&!st.isWindow(i)){for(l=p.delegateType||h,Ot.test(l+h)||(s=s.parentNode);s;s=s.parentNode)d.push(s),u=s;u===(i.ownerDocument||V)&&d.push(u.defaultView||u.parentWindow||e)}for(a=0;(s=d[a++])&&!n.isPropagationStopped();)n.type=a>1?l:p.bindType||h,f=(st._data(s,"events")||{})[n.type]&&st._data(s,"handle"),f&&f.apply(s,r),f=c&&s[c],f&&st.acceptData(s)&&f.apply&&f.apply(s,r)===!1&&n.preventDefault();if(n.type=h,!(o||n.isDefaultPrevented()||p._default&&p._default.apply(i.ownerDocument,r)!==!1||"click"===h&&st.nodeName(i,"a")||!st.acceptData(i)||!c||!i[h]||st.isWindow(i))){u=i[c],u&&(i[c]=null),st.event.triggered=h;try{i[h]()}catch(m){}st.event.triggered=t,u&&(i[c]=u)}return n.result}},dispatch:function(e){e=st.event.fix(e);var n,r,i,o,a,s=[],u=nt.call(arguments),l=(st._data(this,"events")||{})[e.type]||[],c=st.event.special[e.type]||{};if(u[0]=e,e.delegateTarget=this,!c.preDispatch||c.preDispatch.call(this,e)!==!1){for(s=st.event.handlers.call(this,e,l),n=0;(o=s[n++])&&!e.isPropagationStopped();)for(e.currentTarget=o.elem,r=0;(a=o.handlers[r++])&&!e.isImmediatePropagationStopped();)(!e.namespace_re||e.namespace_re.test(a.namespace))&&(e.handleObj=a,e.data=a.data,i=((st.event.special[a.origType]||{}).handle||a.handler).apply(o.elem,u),i!==t&&(e.result=i)===!1&&(e.preventDefault(),e.stopPropagation()));return c.postDispatch&&c.postDispatch.call(this,e),e.result}},handlers:function(e,n){var r,i,o,a,s=[],u=n.delegateCount,l=e.target;if(u&&l.nodeType&&(!e.button||"click"!==e.type))for(;l!=this;l=l.parentNode||this)if(l.disabled!==!0||"click"!==e.type){for(i=[],r=0;u>r;r++)a=n[r],o=a.selector+" ",i[o]===t&&(i[o]=a.needsContext?st(o,this).index(l)>=0:st.find(o,this,null,[l]).length),i[o]&&i.push(a);i.length&&s.push({elem:l,handlers:i})}return n.length>u&&s.push({elem:this,handlers:n.slice(u)}),s},fix:function(e){if(e[st.expando])return e;var t,n,r=e,i=st.event.fixHooks[e.type]||{},o=i.props?this.props.concat(i.props):this.props;for(e=new st.Event(r),t=o.length;t--;)n=o[t],e[n]=r[n];return e.target||(e.target=r.srcElement||V),3===e.target.nodeType&&(e.target=e.target.parentNode),e.metaKey=!!e.metaKey,i.filter?i.filter(e,r):e},props:"altKey bubbles cancelable ctrlKey currentTarget eventPhase metaKey relatedTarget shiftKey target timeStamp view which".split(" "),fixHooks:{},keyHooks:{props:"char charCode key keyCode".split(" "),filter:function(e,t){return null==e.which&&(e.which=null!=t.charCode?t.charCode:t.keyCode),e}},mouseHooks:{props:"button buttons clientX clientY fromElement offsetX offsetY pageX pageY screenX screenY toElement".split(" "),filter:function(e,n){var r,i,o,a=n.button,s=n.fromElement;return null==e.pageX&&null!=n.clientX&&(r=e.target.ownerDocument||V,i=r.documentElement,o=r.body,e.pageX=n.clientX+(i&&i.scrollLeft||o&&o.scrollLeft||0)-(i&&i.clientLeft||o&&o.clientLeft||0),e.pageY=n.clientY+(i&&i.scrollTop||o&&o.scrollTop||0)-(i&&i.clientTop||o&&o.clientTop||0)),!e.relatedTarget&&s&&(e.relatedTarget=s===e.target?n.toElement:s),e.which||a===t||(e.which=1&a?1:2&a?3:4&a?2:0),e}},special:{load:{noBubble:!0},click:{trigger:function(){return st.nodeName(this,"input")&&"checkbox"===this.type&&this.click?(this.click(),!1):t}},focus:{trigger:function(){if(this!==V.activeElement&&this.focus)try{return this.focus(),!1}catch(e){}},delegateType:"focusin"},blur:{trigger:function(){return this===V.activeElement&&this.blur?(this.blur(),!1):t},delegateType:"focusout"},beforeunload:{postDispatch:function(e){e.result!==t&&(e.originalEvent.returnValue=e.result)}}},simulate:function(e,t,n,r){var i=st.extend(new st.Event,n,{type:e,isSimulated:!0,originalEvent:{}});r?st.event.trigger(i,null,t):st.event.dispatch.call(t,i),i.isDefaultPrevented()&&n.preventDefault()}},st.removeEvent=V.removeEventListener?function(e,t,n){e.removeEventListener&&e.removeEventListener(t,n,!1)}:function(e,n,r){var i="on"+n;e.detachEvent&&(e[i]===t&&(e[i]=null),e.detachEvent(i,r))},st.Event=function(e,n){return this instanceof st.Event?(e&&e.type?(this.originalEvent=e,this.type=e.type,this.isDefaultPrevented=e.defaultPrevented||e.returnValue===!1||e.getPreventDefault&&e.getPreventDefault()?u:l):this.type=e,n&&st.extend(this,n),this.timeStamp=e&&e.timeStamp||st.now(),this[st.expando]=!0,t):new st.Event(e,n)},st.Event.prototype={isDefaultPrevented:l,isPropagationStopped:l,isImmediatePropagationStopped:l,preventDefault:function(){var e=this.originalEvent;this.isDefaultPrevented=u,e&&(e.preventDefault?e.preventDefault():e.returnValue=!1)},stopPropagation:function(){var e=this.originalEvent;this.isPropagationStopped=u,e&&(e.stopPropagation&&e.stopPropagation(),e.cancelBubble=!0)},stopImmediatePropagation:function(){this.isImmediatePropagationStopped=u,this.stopPropagation()}},st.each({mouseenter:"mouseover",mouseleave:"mouseout"},function(e,t){st.event.special[e]={delegateType:t,bindType:t,handle:function(e){var n,r=this,i=e.relatedTarget,o=e.handleObj;return(!i||i!==r&&!st.contains(r,i))&&(e.type=o.origType,n=o.handler.apply(this,arguments),e.type=t),n}}}),st.support.submitBubbles||(st.event.special.submit={setup:function(){return st.nodeName(this,"form")?!1:(st.event.add(this,"click._submit keypress._submit",function(e){var n=e.target,r=st.nodeName(n,"input")||st.nodeName(n,"button")?n.form:t;r&&!st._data(r,"submitBubbles")&&(st.event.add(r,"submit._submit",function(e){e._submit_bubble=!0}),st._data(r,"submitBubbles",!0))}),t)},postDispatch:function(e){e._submit_bubble&&(delete e._submit_bubble,this.parentNode&&!e.isTrigger&&st.event.simulate("submit",this.parentNode,e,!0))},teardown:function(){return st.nodeName(this,"form")?!1:(st.event.remove(this,"._submit"),t)}}),st.support.changeBubbles||(st.event.special.change={setup:function(){return qt.test(this.nodeName)?(("checkbox"===this.type||"radio"===this.type)&&(st.event.add(this,"propertychange._change",function(e){"checked"===e.originalEvent.propertyName&&(this._just_changed=!0)}),st.event.add(this,"click._change",function(e){this._just_changed&&!e.isTrigger&&(this._just_changed=!1),st.event.simulate("change",this,e,!0)})),!1):(st.event.add(this,"beforeactivate._change",function(e){var t=e.target;qt.test(t.nodeName)&&!st._data(t,"changeBubbles")&&(st.event.add(t,"change._change",function(e){!this.parentNode||e.isSimulated||e.isTrigger||st.event.simulate("change",this.parentNode,e,!0)}),st._data(t,"changeBubbles",!0))}),t)},handle:function(e){var n=e.target;return this!==n||e.isSimulated||e.isTrigger||"radio"!==n.type&&"checkbox"!==n.type?e.handleObj.handler.apply(this,arguments):t},teardown:function(){return st.event.remove(this,"._change"),!qt.test(this.nodeName)}}),st.support.focusinBubbles||st.each({focus:"focusin",blur:"focusout"},function(e,t){var n=0,r=function(e){st.event.simulate(t,e.target,st.event.fix(e),!0)};st.event.special[t]={setup:function(){0===n++&&V.addEventListener(e,r,!0)},teardown:function(){0===--n&&V.removeEventListener(e,r,!0)}}}),st.fn.extend({on:function(e,n,r,i,o){var a,s;if("object"==typeof e){"string"!=typeof n&&(r=r||n,n=t);for(s in e)this.on(s,n,r,e[s],o);return this}if(null==r&&null==i?(i=n,r=n=t):null==i&&("string"==typeof n?(i=r,r=t):(i=r,r=n,n=t)),i===!1)i=l;else if(!i)return this;return 1===o&&(a=i,i=function(e){return st().off(e),a.apply(this,arguments)},i.guid=a.guid||(a.guid=st.guid++)),this.each(function(){st.event.add(this,e,i,r,n)})},one:function(e,t,n,r){return this.on(e,t,n,r,1)},off:function(e,n,r){var i,o;if(e&&e.preventDefault&&e.handleObj)return i=e.handleObj,st(e.delegateTarget).off(i.namespace?i.origType+"."+i.namespace:i.origType,i.selector,i.handler),this;if("object"==typeof e){for(o in e)this.off(o,n,e[o]);return this}return(n===!1||"function"==typeof n)&&(r=n,n=t),r===!1&&(r=l),this.each(function(){st.event.remove(this,e,r,n)})},bind:function(e,t,n){return this.on(e,null,t,n)},unbind:function(e,t){return this.off(e,null,t)},delegate:function(e,t,n,r){return this.on(t,e,n,r)},undelegate:function(e,t,n){return 1===arguments.length?this.off(e,"**"):this.off(t,e||"**",n)},trigger:function(e,t){return this.each(function(){st.event.trigger(e,t,this)})},triggerHandler:function(e,n){var r=this[0];return r?st.event.trigger(e,n,r,!0):t},hover:function(e,t){return this.mouseenter(e).mouseleave(t||e)}}),st.each("blur focus focusin focusout load resize scroll unload click dblclick mousedown mouseup mousemove mouseover mouseout mouseenter mouseleave change select submit keydown keypress keyup error contextmenu".split(" "),function(e,t){st.fn[t]=function(e,n){return arguments.length>0?this.on(t,null,e,n):this.trigger(t)},_t.test(t)&&(st.event.fixHooks[t]=st.event.keyHooks),Ft.test(t)&&(st.event.fixHooks[t]=st.event.mouseHooks)}),function(e,t){function n(e){return ht.test(e+"")}function r(){var e,t=[];return e=function(n,r){return t.push(n+=" ")>C.cacheLength&&delete e[t.shift()],e[n]=r}}function i(e){return e[P]=!0,e}function o(e){var t=L.createElement("div");try{return e(t)}catch(n){return!1}finally{t=null}}function a(e,t,n,r){var i,o,a,s,u,l,c,d,h,g;if((t?t.ownerDocument||t:R)!==L&&D(t),t=t||L,n=n||[],!e||"string"!=typeof e)return n;if(1!==(s=t.nodeType)&&9!==s)return[];if(!M&&!r){if(i=gt.exec(e))if(a=i[1]){if(9===s){if(o=t.getElementById(a),!o||!o.parentNode)return n;if(o.id===a)return n.push(o),n}else if(t.ownerDocument&&(o=t.ownerDocument.getElementById(a))&&O(t,o)&&o.id===a)return n.push(o),n}else{if(i[2])return Q.apply(n,K.call(t.getElementsByTagName(e),0)),n;if((a=i[3])&&W.getByClassName&&t.getElementsByClassName)return Q.apply(n,K.call(t.getElementsByClassName(a),0)),n}if(W.qsa&&!q.test(e)){if(c=!0,d=P,h=t,g=9===s&&e,1===s&&"object"!==t.nodeName.toLowerCase()){for(l=f(e),(c=t.getAttribute("id"))?d=c.replace(vt,"\\$&"):t.setAttribute("id",d),d="[id='"+d+"'] ",u=l.length;u--;)l[u]=d+p(l[u]);h=dt.test(e)&&t.parentNode||t,g=l.join(",")}if(g)try{return Q.apply(n,K.call(h.querySelectorAll(g),0)),n}catch(m){}finally{c||t.removeAttribute("id")}}}return x(e.replace(at,"$1"),t,n,r)}function s(e,t){for(var n=e&&t&&e.nextSibling;n;n=n.nextSibling)if(n===t)return-1;return e?1:-1}function u(e){return function(t){var n=t.nodeName.toLowerCase();return"input"===n&&t.type===e}}function l(e){return function(t){var n=t.nodeName.toLowerCase();return("input"===n||"button"===n)&&t.type===e}}function c(e){return i(function(t){return t=+t,i(function(n,r){for(var i,o=e([],n.length,t),a=o.length;a--;)n[i=o[a]]&&(n[i]=!(r[i]=n[i]))})})}function f(e,t){var n,r,i,o,s,u,l,c=X[e+" "];if(c)return t?0:c.slice(0);for(s=e,u=[],l=C.preFilter;s;){(!n||(r=ut.exec(s)))&&(r&&(s=s.slice(r[0].length)||s),u.push(i=[])),n=!1,(r=lt.exec(s))&&(n=r.shift(),i.push({value:n,type:r[0].replace(at," ")}),s=s.slice(n.length));for(o in C.filter)!(r=pt[o].exec(s))||l[o]&&!(r=l[o](r))||(n=r.shift(),i.push({value:n,type:o,matches:r}),s=s.slice(n.length));if(!n)break}return t?s.length:s?a.error(e):X(e,u).slice(0)}function p(e){for(var t=0,n=e.length,r="";n>t;t++)r+=e[t].value;return r}function d(e,t,n){var r=t.dir,i=n&&"parentNode"===t.dir,o=I++;return t.first?function(t,n,o){for(;t=t[r];)if(1===t.nodeType||i)return e(t,n,o)}:function(t,n,a){var s,u,l,c=$+" "+o;if(a){for(;t=t[r];)if((1===t.nodeType||i)&&e(t,n,a))return!0}else for(;t=t[r];)if(1===t.nodeType||i)if(l=t[P]||(t[P]={}),(u=l[r])&&u[0]===c){if((s=u[1])===!0||s===N)return s===!0}else if(u=l[r]=[c],u[1]=e(t,n,a)||N,u[1]===!0)return!0}}function h(e){return e.length>1?function(t,n,r){for(var i=e.length;i--;)if(!e[i](t,n,r))return!1;return!0}:e[0]}function g(e,t,n,r,i){for(var o,a=[],s=0,u=e.length,l=null!=t;u>s;s++)(o=e[s])&&(!n||n(o,r,i))&&(a.push(o),l&&t.push(s));return a}function m(e,t,n,r,o,a){return r&&!r[P]&&(r=m(r)),o&&!o[P]&&(o=m(o,a)),i(function(i,a,s,u){var l,c,f,p=[],d=[],h=a.length,m=i||b(t||"*",s.nodeType?[s]:s,[]),y=!e||!i&&t?m:g(m,p,e,s,u),v=n?o||(i?e:h||r)?[]:a:y;if(n&&n(y,v,s,u),r)for(l=g(v,d),r(l,[],s,u),c=l.length;c--;)(f=l[c])&&(v[d[c]]=!(y[d[c]]=f));if(i){if(o||e){if(o){for(l=[],c=v.length;c--;)(f=v[c])&&l.push(y[c]=f);o(null,v=[],l,u)}for(c=v.length;c--;)(f=v[c])&&(l=o?Z.call(i,f):p[c])>-1&&(i[l]=!(a[l]=f))}}else v=g(v===a?v.splice(h,v.length):v),o?o(null,a,v,u):Q.apply(a,v)})}function y(e){for(var t,n,r,i=e.length,o=C.relative[e[0].type],a=o||C.relative[" "],s=o?1:0,u=d(function(e){return e===t},a,!0),l=d(function(e){return Z.call(t,e)>-1},a,!0),c=[function(e,n,r){return!o&&(r||n!==j)||((t=n).nodeType?u(e,n,r):l(e,n,r))}];i>s;s++)if(n=C.relative[e[s].type])c=[d(h(c),n)];else{if(n=C.filter[e[s].type].apply(null,e[s].matches),n[P]){for(r=++s;i>r&&!C.relative[e[r].type];r++);return m(s>1&&h(c),s>1&&p(e.slice(0,s-1)).replace(at,"$1"),n,r>s&&y(e.slice(s,r)),i>r&&y(e=e.slice(r)),i>r&&p(e))}c.push(n)}return h(c)}function v(e,t){var n=0,r=t.length>0,o=e.length>0,s=function(i,s,u,l,c){var f,p,d,h=[],m=0,y="0",v=i&&[],b=null!=c,x=j,T=i||o&&C.find.TAG("*",c&&s.parentNode||s),w=$+=null==x?1:Math.E;for(b&&(j=s!==L&&s,N=n);null!=(f=T[y]);y++){if(o&&f){for(p=0;d=e[p];p++)if(d(f,s,u)){l.push(f);break}b&&($=w,N=++n)}r&&((f=!d&&f)&&m--,i&&v.push(f))}if(m+=y,r&&y!==m){for(p=0;d=t[p];p++)d(v,h,s,u);if(i){if(m>0)for(;y--;)v[y]||h[y]||(h[y]=G.call(l));h=g(h)}Q.apply(l,h),b&&!i&&h.length>0&&m+t.length>1&&a.uniqueSort(l)}return b&&($=w,j=x),v};return r?i(s):s}function b(e,t,n){for(var r=0,i=t.length;i>r;r++)a(e,t[r],n);return n}function x(e,t,n,r){var i,o,a,s,u,l=f(e);if(!r&&1===l.length){if(o=l[0]=l[0].slice(0),o.length>2&&"ID"===(a=o[0]).type&&9===t.nodeType&&!M&&C.relative[o[1].type]){if(t=C.find.ID(a.matches[0].replace(xt,Tt),t)[0],!t)return n;e=e.slice(o.shift().value.length)}for(i=pt.needsContext.test(e)?-1:o.length-1;i>=0&&(a=o[i],!C.relative[s=a.type]);i--)if((u=C.find[s])&&(r=u(a.matches[0].replace(xt,Tt),dt.test(o[0].type)&&t.parentNode||t))){if(o.splice(i,1),e=r.length&&p(o),!e)return Q.apply(n,K.call(r,0)),n;break}}return S(e,l)(r,t,M,n,dt.test(e)),n}function T(){}var w,N,C,k,E,S,A,j,D,L,H,M,q,_,F,O,B,P="sizzle"+-new Date,R=e.document,W={},$=0,I=0,z=r(),X=r(),U=r(),V=typeof t,Y=1<<31,J=[],G=J.pop,Q=J.push,K=J.slice,Z=J.indexOf||function(e){for(var t=0,n=this.length;n>t;t++)if(this[t]===e)return t;return-1},et="[\\x20\\t\\r\\n\\f]",tt="(?:\\\\.|[\\w-]|[^\\x00-\\xa0])+",nt=tt.replace("w","w#"),rt="([*^$|!~]?=)",it="\\["+et+"*("+tt+")"+et+"*(?:"+rt+et+"*(?:(['\"])((?:\\\\.|[^\\\\])*?)\\3|("+nt+")|)|)"+et+"*\\]",ot=":("+tt+")(?:\\(((['\"])((?:\\\\.|[^\\\\])*?)\\3|((?:\\\\.|[^\\\\()[\\]]|"+it.replace(3,8)+")*)|.*)\\)|)",at=RegExp("^"+et+"+|((?:^|[^\\\\])(?:\\\\.)*)"+et+"+$","g"),ut=RegExp("^"+et+"*,"+et+"*"),lt=RegExp("^"+et+"*([\\x20\\t\\r\\n\\f>+~])"+et+"*"),ct=RegExp(ot),ft=RegExp("^"+nt+"$"),pt={ID:RegExp("^#("+tt+")"),CLASS:RegExp("^\\.("+tt+")"),NAME:RegExp("^\\[name=['\"]?("+tt+")['\"]?\\]"),TAG:RegExp("^("+tt.replace("w","w*")+")"),ATTR:RegExp("^"+it),PSEUDO:RegExp("^"+ot),CHILD:RegExp("^:(only|first|last|nth|nth-last)-(child|of-type)(?:\\("+et+"*(even|odd|(([+-]|)(\\d*)n|)"+et+"*(?:([+-]|)"+et+"*(\\d+)|))"+et+"*\\)|)","i"),needsContext:RegExp("^"+et+"*[>+~]|:(even|odd|eq|gt|lt|nth|first|last)(?:\\("+et+"*((?:-\\d)?\\d*)"+et+"*\\)|)(?=[^-]|$)","i")},dt=/[\x20\t\r\n\f]*[+~]/,ht=/\{\s*\[native code\]\s*\}/,gt=/^(?:#([\w-]+)|(\w+)|\.([\w-]+))$/,mt=/^(?:input|select|textarea|button)$/i,yt=/^h\d$/i,vt=/'|\\/g,bt=/\=[\x20\t\r\n\f]*([^'"\]]*)[\x20\t\r\n\f]*\]/g,xt=/\\([\da-fA-F]{1,6}[\x20\t\r\n\f]?|.)/g,Tt=function(e,t){var n="0x"+t-65536;return n!==n?t:0>n?String.fromCharCode(n+65536):String.fromCharCode(55296|n>>10,56320|1023&n)};try{K.call(H.childNodes,0)[0].nodeType}catch(wt){K=function(e){for(var t,n=[];t=this[e];e++)n.push(t);return n}}E=a.isXML=function(e){var t=e&&(e.ownerDocument||e).documentElement;return t?"HTML"!==t.nodeName:!1},D=a.setDocument=function(e){var r=e?e.ownerDocument||e:R;return r!==L&&9===r.nodeType&&r.documentElement?(L=r,H=r.documentElement,M=E(r),W.tagNameNoComments=o(function(e){return e.appendChild(r.createComment("")),!e.getElementsByTagName("*").length}),W.attributes=o(function(e){e.innerHTML="<select></select>";var t=typeof e.lastChild.getAttribute("multiple");return"boolean"!==t&&"string"!==t}),W.getByClassName=o(function(e){return e.innerHTML="<div class='hidden e'></div><div class='hidden'></div>",e.getElementsByClassName&&e.getElementsByClassName("e").length?(e.lastChild.className="e",2===e.getElementsByClassName("e").length):!1}),W.getByName=o(function(e){e.id=P+0,e.innerHTML="<a name='"+P+"'></a><div name='"+P+"'></div>",H.insertBefore(e,H.firstChild);var t=r.getElementsByName&&r.getElementsByName(P).length===2+r.getElementsByName(P+0).length;return W.getIdNotName=!r.getElementById(P),H.removeChild(e),t}),C.attrHandle=o(function(e){return e.innerHTML="<a href='#'></a>",e.firstChild&&typeof e.firstChild.getAttribute!==V&&"#"===e.firstChild.getAttribute("href")})?{}:{href:function(e){return e.getAttribute("href",2)},type:function(e){return e.getAttribute("type")}},W.getIdNotName?(C.find.ID=function(e,t){if(typeof t.getElementById!==V&&!M){var n=t.getElementById(e);return n&&n.parentNode?[n]:[]}},C.filter.ID=function(e){var t=e.replace(xt,Tt);return function(e){return e.getAttribute("id")===t}}):(C.find.ID=function(e,n){if(typeof n.getElementById!==V&&!M){var r=n.getElementById(e);return r?r.id===e||typeof r.getAttributeNode!==V&&r.getAttributeNode("id").value===e?[r]:t:[]}},C.filter.ID=function(e){var t=e.replace(xt,Tt);return function(e){var n=typeof e.getAttributeNode!==V&&e.getAttributeNode("id");return n&&n.value===t}}),C.find.TAG=W.tagNameNoComments?function(e,n){return typeof n.getElementsByTagName!==V?n.getElementsByTagName(e):t}:function(e,t){var n,r=[],i=0,o=t.getElementsByTagName(e);if("*"===e){for(;n=o[i];i++)1===n.nodeType&&r.push(n);return r}return o},C.find.NAME=W.getByName&&function(e,n){return typeof n.getElementsByName!==V?n.getElementsByName(name):t},C.find.CLASS=W.getByClassName&&function(e,n){return typeof n.getElementsByClassName===V||M?t:n.getElementsByClassName(e)},_=[],q=[":focus"],(W.qsa=n(r.querySelectorAll))&&(o(function(e){e.innerHTML="<select><option selected=''></option></select>",e.querySelectorAll("[selected]").length||q.push("\\["+et+"*(?:checked|disabled|ismap|multiple|readonly|selected|value)"),e.querySelectorAll(":checked").length||q.push(":checked")}),o(function(e){e.innerHTML="<input type='hidden' i=''/>",e.querySelectorAll("[i^='']").length&&q.push("[*^$]="+et+"*(?:\"\"|'')"),e.querySelectorAll(":enabled").length||q.push(":enabled",":disabled"),e.querySelectorAll("*,:x"),q.push(",.*:")})),(W.matchesSelector=n(F=H.matchesSelector||H.mozMatchesSelector||H.webkitMatchesSelector||H.oMatchesSelector||H.msMatchesSelector))&&o(function(e){W.disconnectedMatch=F.call(e,"div"),F.call(e,"[s!='']:x"),_.push("!=",ot)}),q=RegExp(q.join("|")),_=RegExp(_.join("|")),O=n(H.contains)||H.compareDocumentPosition?function(e,t){var n=9===e.nodeType?e.documentElement:e,r=t&&t.parentNode;return e===r||!(!r||1!==r.nodeType||!(n.contains?n.contains(r):e.compareDocumentPosition&&16&e.compareDocumentPosition(r)))}:function(e,t){if(t)for(;t=t.parentNode;)if(t===e)return!0;return!1},B=H.compareDocumentPosition?function(e,t){var n;return e===t?(A=!0,0):(n=t.compareDocumentPosition&&e.compareDocumentPosition&&e.compareDocumentPosition(t))?1&n||e.parentNode&&11===e.parentNode.nodeType?e===r||O(R,e)?-1:t===r||O(R,t)?1:0:4&n?-1:1:e.compareDocumentPosition?-1:1}:function(e,t){var n,i=0,o=e.parentNode,a=t.parentNode,u=[e],l=[t];if(e===t)return A=!0,0;if(e.sourceIndex&&t.sourceIndex)return(~t.sourceIndex||Y)-(O(R,e)&&~e.sourceIndex||Y);if(!o||!a)return e===r?-1:t===r?1:o?-1:a?1:0;if(o===a)return s(e,t);for(n=e;n=n.parentNode;)u.unshift(n);for(n=t;n=n.parentNode;)l.unshift(n);for(;u[i]===l[i];)i++;return i?s(u[i],l[i]):u[i]===R?-1:l[i]===R?1:0},A=!1,[0,0].sort(B),W.detectDuplicates=A,L):L},a.matches=function(e,t){return a(e,null,null,t)},a.matchesSelector=function(e,t){if((e.ownerDocument||e)!==L&&D(e),t=t.replace(bt,"='$1']"),!(!W.matchesSelector||M||_&&_.test(t)||q.test(t)))try{var n=F.call(e,t);if(n||W.disconnectedMatch||e.document&&11!==e.document.nodeType)return n}catch(r){}return a(t,L,null,[e]).length>0},a.contains=function(e,t){return(e.ownerDocument||e)!==L&&D(e),O(e,t)},a.attr=function(e,t){var n;return(e.ownerDocument||e)!==L&&D(e),M||(t=t.toLowerCase()),(n=C.attrHandle[t])?n(e):M||W.attributes?e.getAttribute(t):((n=e.getAttributeNode(t))||e.getAttribute(t))&&e[t]===!0?t:n&&n.specified?n.value:null},a.error=function(e){throw Error("Syntax error, unrecognized expression: "+e)},a.uniqueSort=function(e){var t,n=[],r=1,i=0;if(A=!W.detectDuplicates,e.sort(B),A){for(;t=e[r];r++)t===e[r-1]&&(i=n.push(r));for(;i--;)e.splice(n[i],1)}return e},k=a.getText=function(e){var t,n="",r=0,i=e.nodeType;if(i){if(1===i||9===i||11===i){if("string"==typeof e.textContent)return e.textContent;for(e=e.firstChild;e;e=e.nextSibling)n+=k(e)}else if(3===i||4===i)return e.nodeValue}else for(;t=e[r];r++)n+=k(t);return n},C=a.selectors={cacheLength:50,createPseudo:i,match:pt,find:{},relative:{">":{dir:"parentNode",first:!0}," ":{dir:"parentNode"},"+":{dir:"previousSibling",first:!0},"~":{dir:"previousSibling"}},preFilter:{ATTR:function(e){return e[1]=e[1].replace(xt,Tt),e[3]=(e[4]||e[5]||"").replace(xt,Tt),"~="===e[2]&&(e[3]=" "+e[3]+" "),e.slice(0,4)},CHILD:function(e){return e[1]=e[1].toLowerCase(),"nth"===e[1].slice(0,3)?(e[3]||a.error(e[0]),e[4]=+(e[4]?e[5]+(e[6]||1):2*("even"===e[3]||"odd"===e[3])),e[5]=+(e[7]+e[8]||"odd"===e[3])):e[3]&&a.error(e[0]),e},PSEUDO:function(e){var t,n=!e[5]&&e[2];return pt.CHILD.test(e[0])?null:(e[4]?e[2]=e[4]:n&&ct.test(n)&&(t=f(n,!0))&&(t=n.indexOf(")",n.length-t)-n.length)&&(e[0]=e[0].slice(0,t),e[2]=n.slice(0,t)),e.slice(0,3))}},filter:{TAG:function(e){return"*"===e?function(){return!0}:(e=e.replace(xt,Tt).toLowerCase(),function(t){return t.nodeName&&t.nodeName.toLowerCase()===e})},CLASS:function(e){var t=z[e+" "];return t||(t=RegExp("(^|"+et+")"+e+"("+et+"|$)"))&&z(e,function(e){return t.test(e.className||typeof e.getAttribute!==V&&e.getAttribute("class")||"")})},ATTR:function(e,t,n){return function(r){var i=a.attr(r,e);return null==i?"!="===t:t?(i+="","="===t?i===n:"!="===t?i!==n:"^="===t?n&&0===i.indexOf(n):"*="===t?n&&i.indexOf(n)>-1:"$="===t?n&&i.substr(i.length-n.length)===n:"~="===t?(" "+i+" ").indexOf(n)>-1:"|="===t?i===n||i.substr(0,n.length+1)===n+"-":!1):!0}},CHILD:function(e,t,n,r,i){var o="nth"!==e.slice(0,3),a="last"!==e.slice(-4),s="of-type"===t;return 1===r&&0===i?function(e){return!!e.parentNode}:function(t,n,u){var l,c,f,p,d,h,g=o!==a?"nextSibling":"previousSibling",m=t.parentNode,y=s&&t.nodeName.toLowerCase(),v=!u&&!s;if(m){if(o){for(;g;){for(f=t;f=f[g];)if(s?f.nodeName.toLowerCase()===y:1===f.nodeType)return!1;h=g="only"===e&&!h&&"nextSibling"}return!0}if(h=[a?m.firstChild:m.lastChild],a&&v){for(c=m[P]||(m[P]={}),l=c[e]||[],d=l[0]===$&&l[1],p=l[0]===$&&l[2],f=d&&m.childNodes[d];f=++d&&f&&f[g]||(p=d=0)||h.pop();)if(1===f.nodeType&&++p&&f===t){c[e]=[$,d,p];break}}else if(v&&(l=(t[P]||(t[P]={}))[e])&&l[0]===$)p=l[1];else for(;(f=++d&&f&&f[g]||(p=d=0)||h.pop())&&((s?f.nodeName.toLowerCase()!==y:1!==f.nodeType)||!++p||(v&&((f[P]||(f[P]={}))[e]=[$,p]),f!==t)););return p-=i,p===r||0===p%r&&p/r>=0}}},PSEUDO:function(e,t){var n,r=C.pseudos[e]||C.setFilters[e.toLowerCase()]||a.error("unsupported pseudo: "+e);return r[P]?r(t):r.length>1?(n=[e,e,"",t],C.setFilters.hasOwnProperty(e.toLowerCase())?i(function(e,n){for(var i,o=r(e,t),a=o.length;a--;)i=Z.call(e,o[a]),e[i]=!(n[i]=o[a])}):function(e){return r(e,0,n)}):r}},pseudos:{not:i(function(e){var t=[],n=[],r=S(e.replace(at,"$1"));return r[P]?i(function(e,t,n,i){for(var o,a=r(e,null,i,[]),s=e.length;s--;)(o=a[s])&&(e[s]=!(t[s]=o))}):function(e,i,o){return t[0]=e,r(t,null,o,n),!n.pop()}}),has:i(function(e){return function(t){return a(e,t).length>0}}),contains:i(function(e){return function(t){return(t.textContent||t.innerText||k(t)).indexOf(e)>-1}}),lang:i(function(e){return ft.test(e||"")||a.error("unsupported lang: "+e),e=e.replace(xt,Tt).toLowerCase(),function(t){var n;do if(n=M?t.getAttribute("xml:lang")||t.getAttribute("lang"):t.lang)return n=n.toLowerCase(),n===e||0===n.indexOf(e+"-");while((t=t.parentNode)&&1===t.nodeType);return!1}}),target:function(t){var n=e.location&&e.location.hash;return n&&n.slice(1)===t.id},root:function(e){return e===H},focus:function(e){return e===L.activeElement&&(!L.hasFocus||L.hasFocus())&&!!(e.type||e.href||~e.tabIndex)},enabled:function(e){return e.disabled===!1},disabled:function(e){return e.disabled===!0},checked:function(e){var t=e.nodeName.toLowerCase();return"input"===t&&!!e.checked||"option"===t&&!!e.selected},selected:function(e){return e.parentNode&&e.parentNode.selectedIndex,e.selected===!0},empty:function(e){for(e=e.firstChild;e;e=e.nextSibling)if(e.nodeName>"@"||3===e.nodeType||4===e.nodeType)return!1;return!0},parent:function(e){return!C.pseudos.empty(e)},header:function(e){return yt.test(e.nodeName)},input:function(e){return mt.test(e.nodeName)},button:function(e){var t=e.nodeName.toLowerCase();return"input"===t&&"button"===e.type||"button"===t},text:function(e){var t;return"input"===e.nodeName.toLowerCase()&&"text"===e.type&&(null==(t=e.getAttribute("type"))||t.toLowerCase()===e.type)},first:c(function(){return[0]}),last:c(function(e,t){return[t-1]}),eq:c(function(e,t,n){return[0>n?n+t:n]}),even:c(function(e,t){for(var n=0;t>n;n+=2)e.push(n);return e}),odd:c(function(e,t){for(var n=1;t>n;n+=2)e.push(n);return e}),lt:c(function(e,t,n){for(var r=0>n?n+t:n;--r>=0;)e.push(r);return e}),gt:c(function(e,t,n){for(var r=0>n?n+t:n;t>++r;)e.push(r);return e})}};for(w in{radio:!0,checkbox:!0,file:!0,password:!0,image:!0})C.pseudos[w]=u(w);for(w in{submit:!0,reset:!0})C.pseudos[w]=l(w);S=a.compile=function(e,t){var n,r=[],i=[],o=U[e+" "];if(!o){for(t||(t=f(e)),n=t.length;n--;)o=y(t[n]),o[P]?r.push(o):i.push(o);o=U(e,v(i,r))}return o},C.pseudos.nth=C.pseudos.eq,C.filters=T.prototype=C.pseudos,C.setFilters=new T,D(),a.attr=st.attr,st.find=a,st.expr=a.selectors,st.expr[":"]=st.expr.pseudos,st.unique=a.uniqueSort,st.text=a.getText,st.isXMLDoc=a.isXML,st.contains=a.contains}(e);var Pt=/Until$/,Rt=/^(?:parents|prev(?:Until|All))/,Wt=/^.[^:#\[\.,]*$/,$t=st.expr.match.needsContext,It={children:!0,contents:!0,next:!0,prev:!0};st.fn.extend({find:function(e){var t,n,r;if("string"!=typeof e)return r=this,this.pushStack(st(e).filter(function(){for(t=0;r.length>t;t++)if(st.contains(r[t],this))return!0}));for(n=[],t=0;this.length>t;t++)st.find(e,this[t],n);return n=this.pushStack(st.unique(n)),n.selector=(this.selector?this.selector+" ":"")+e,n},has:function(e){var t,n=st(e,this),r=n.length;return this.filter(function(){for(t=0;r>t;t++)if(st.contains(this,n[t]))return!0})},not:function(e){return this.pushStack(f(this,e,!1))},filter:function(e){return this.pushStack(f(this,e,!0))},is:function(e){return!!e&&("string"==typeof e?$t.test(e)?st(e,this.context).index(this[0])>=0:st.filter(e,this).length>0:this.filter(e).length>0)},closest:function(e,t){for(var n,r=0,i=this.length,o=[],a=$t.test(e)||"string"!=typeof e?st(e,t||this.context):0;i>r;r++)for(n=this[r];n&&n.ownerDocument&&n!==t&&11!==n.nodeType;){if(a?a.index(n)>-1:st.find.matchesSelector(n,e)){o.push(n);break}n=n.parentNode}return this.pushStack(o.length>1?st.unique(o):o)},index:function(e){return e?"string"==typeof e?st.inArray(this[0],st(e)):st.inArray(e.jquery?e[0]:e,this):this[0]&&this[0].parentNode?this.first().prevAll().length:-1},add:function(e,t){var n="string"==typeof e?st(e,t):st.makeArray(e&&e.nodeType?[e]:e),r=st.merge(this.get(),n);return this.pushStack(st.unique(r))},addBack:function(e){return this.add(null==e?this.prevObject:this.prevObject.filter(e))}}),st.fn.andSelf=st.fn.addBack,st.each({parent:function(e){var t=e.parentNode;return t&&11!==t.nodeType?t:null},parents:function(e){return st.dir(e,"parentNode")},parentsUntil:function(e,t,n){return st.dir(e,"parentNode",n)},next:function(e){return c(e,"nextSibling")},prev:function(e){return c(e,"previousSibling")
},nextAll:function(e){return st.dir(e,"nextSibling")},prevAll:function(e){return st.dir(e,"previousSibling")},nextUntil:function(e,t,n){return st.dir(e,"nextSibling",n)},prevUntil:function(e,t,n){return st.dir(e,"previousSibling",n)},siblings:function(e){return st.sibling((e.parentNode||{}).firstChild,e)},children:function(e){return st.sibling(e.firstChild)},contents:function(e){return st.nodeName(e,"iframe")?e.contentDocument||e.contentWindow.document:st.merge([],e.childNodes)}},function(e,t){st.fn[e]=function(n,r){var i=st.map(this,t,n);return Pt.test(e)||(r=n),r&&"string"==typeof r&&(i=st.filter(r,i)),i=this.length>1&&!It[e]?st.unique(i):i,this.length>1&&Rt.test(e)&&(i=i.reverse()),this.pushStack(i)}}),st.extend({filter:function(e,t,n){return n&&(e=":not("+e+")"),1===t.length?st.find.matchesSelector(t[0],e)?[t[0]]:[]:st.find.matches(e,t)},dir:function(e,n,r){for(var i=[],o=e[n];o&&9!==o.nodeType&&(r===t||1!==o.nodeType||!st(o).is(r));)1===o.nodeType&&i.push(o),o=o[n];return i},sibling:function(e,t){for(var n=[];e;e=e.nextSibling)1===e.nodeType&&e!==t&&n.push(e);return n}});var zt="abbr|article|aside|audio|bdi|canvas|data|datalist|details|figcaption|figure|footer|header|hgroup|mark|meter|nav|output|progress|section|summary|time|video",Xt=/ jQuery\d+="(?:null|\d+)"/g,Ut=RegExp("<(?:"+zt+")[\\s/>]","i"),Vt=/^\s+/,Yt=/<(?!area|br|col|embed|hr|img|input|link|meta|param)(([\w:]+)[^>]*)\/>/gi,Jt=/<([\w:]+)/,Gt=/<tbody/i,Qt=/<|&#?\w+;/,Kt=/<(?:script|style|link)/i,Zt=/^(?:checkbox|radio)$/i,en=/checked\s*(?:[^=]|=\s*.checked.)/i,tn=/^$|\/(?:java|ecma)script/i,nn=/^true\/(.*)/,rn=/^\s*<!(?:\[CDATA\[|--)|(?:\]\]|--)>\s*$/g,on={option:[1,"<select multiple='multiple'>","</select>"],legend:[1,"<fieldset>","</fieldset>"],area:[1,"<map>","</map>"],param:[1,"<object>","</object>"],thead:[1,"<table>","</table>"],tr:[2,"<table><tbody>","</tbody></table>"],col:[2,"<table><tbody></tbody><colgroup>","</colgroup></table>"],td:[3,"<table><tbody><tr>","</tr></tbody></table>"],_default:st.support.htmlSerialize?[0,"",""]:[1,"X<div>","</div>"]},an=p(V),sn=an.appendChild(V.createElement("div"));on.optgroup=on.option,on.tbody=on.tfoot=on.colgroup=on.caption=on.thead,on.th=on.td,st.fn.extend({text:function(e){return st.access(this,function(e){return e===t?st.text(this):this.empty().append((this[0]&&this[0].ownerDocument||V).createTextNode(e))},null,e,arguments.length)},wrapAll:function(e){if(st.isFunction(e))return this.each(function(t){st(this).wrapAll(e.call(this,t))});if(this[0]){var t=st(e,this[0].ownerDocument).eq(0).clone(!0);this[0].parentNode&&t.insertBefore(this[0]),t.map(function(){for(var e=this;e.firstChild&&1===e.firstChild.nodeType;)e=e.firstChild;return e}).append(this)}return this},wrapInner:function(e){return st.isFunction(e)?this.each(function(t){st(this).wrapInner(e.call(this,t))}):this.each(function(){var t=st(this),n=t.contents();n.length?n.wrapAll(e):t.append(e)})},wrap:function(e){var t=st.isFunction(e);return this.each(function(n){st(this).wrapAll(t?e.call(this,n):e)})},unwrap:function(){return this.parent().each(function(){st.nodeName(this,"body")||st(this).replaceWith(this.childNodes)}).end()},append:function(){return this.domManip(arguments,!0,function(e){(1===this.nodeType||11===this.nodeType||9===this.nodeType)&&this.appendChild(e)})},prepend:function(){return this.domManip(arguments,!0,function(e){(1===this.nodeType||11===this.nodeType||9===this.nodeType)&&this.insertBefore(e,this.firstChild)})},before:function(){return this.domManip(arguments,!1,function(e){this.parentNode&&this.parentNode.insertBefore(e,this)})},after:function(){return this.domManip(arguments,!1,function(e){this.parentNode&&this.parentNode.insertBefore(e,this.nextSibling)})},remove:function(e,t){for(var n,r=0;null!=(n=this[r]);r++)(!e||st.filter(e,[n]).length>0)&&(t||1!==n.nodeType||st.cleanData(b(n)),n.parentNode&&(t&&st.contains(n.ownerDocument,n)&&m(b(n,"script")),n.parentNode.removeChild(n)));return this},empty:function(){for(var e,t=0;null!=(e=this[t]);t++){for(1===e.nodeType&&st.cleanData(b(e,!1));e.firstChild;)e.removeChild(e.firstChild);e.options&&st.nodeName(e,"select")&&(e.options.length=0)}return this},clone:function(e,t){return e=null==e?!1:e,t=null==t?e:t,this.map(function(){return st.clone(this,e,t)})},html:function(e){return st.access(this,function(e){var n=this[0]||{},r=0,i=this.length;if(e===t)return 1===n.nodeType?n.innerHTML.replace(Xt,""):t;if(!("string"!=typeof e||Kt.test(e)||!st.support.htmlSerialize&&Ut.test(e)||!st.support.leadingWhitespace&&Vt.test(e)||on[(Jt.exec(e)||["",""])[1].toLowerCase()])){e=e.replace(Yt,"<$1></$2>");try{for(;i>r;r++)n=this[r]||{},1===n.nodeType&&(st.cleanData(b(n,!1)),n.innerHTML=e);n=0}catch(o){}}n&&this.empty().append(e)},null,e,arguments.length)},replaceWith:function(e){var t=st.isFunction(e);return t||"string"==typeof e||(e=st(e).not(this).detach()),this.domManip([e],!0,function(e){var t=this.nextSibling,n=this.parentNode;(n&&1===this.nodeType||11===this.nodeType)&&(st(this).remove(),t?t.parentNode.insertBefore(e,t):n.appendChild(e))})},detach:function(e){return this.remove(e,!0)},domManip:function(e,n,r){e=et.apply([],e);var i,o,a,s,u,l,c=0,f=this.length,p=this,m=f-1,y=e[0],v=st.isFunction(y);if(v||!(1>=f||"string"!=typeof y||st.support.checkClone)&&en.test(y))return this.each(function(i){var o=p.eq(i);v&&(e[0]=y.call(this,i,n?o.html():t)),o.domManip(e,n,r)});if(f&&(i=st.buildFragment(e,this[0].ownerDocument,!1,this),o=i.firstChild,1===i.childNodes.length&&(i=o),o)){for(n=n&&st.nodeName(o,"tr"),a=st.map(b(i,"script"),h),s=a.length;f>c;c++)u=i,c!==m&&(u=st.clone(u,!0,!0),s&&st.merge(a,b(u,"script"))),r.call(n&&st.nodeName(this[c],"table")?d(this[c],"tbody"):this[c],u,c);if(s)for(l=a[a.length-1].ownerDocument,st.map(a,g),c=0;s>c;c++)u=a[c],tn.test(u.type||"")&&!st._data(u,"globalEval")&&st.contains(l,u)&&(u.src?st.ajax({url:u.src,type:"GET",dataType:"script",async:!1,global:!1,"throws":!0}):st.globalEval((u.text||u.textContent||u.innerHTML||"").replace(rn,"")));i=o=null}return this}}),st.each({appendTo:"append",prependTo:"prepend",insertBefore:"before",insertAfter:"after",replaceAll:"replaceWith"},function(e,t){st.fn[e]=function(e){for(var n,r=0,i=[],o=st(e),a=o.length-1;a>=r;r++)n=r===a?this:this.clone(!0),st(o[r])[t](n),tt.apply(i,n.get());return this.pushStack(i)}}),st.extend({clone:function(e,t,n){var r,i,o,a,s,u=st.contains(e.ownerDocument,e);if(st.support.html5Clone||st.isXMLDoc(e)||!Ut.test("<"+e.nodeName+">")?s=e.cloneNode(!0):(sn.innerHTML=e.outerHTML,sn.removeChild(s=sn.firstChild)),!(st.support.noCloneEvent&&st.support.noCloneChecked||1!==e.nodeType&&11!==e.nodeType||st.isXMLDoc(e)))for(r=b(s),i=b(e),a=0;null!=(o=i[a]);++a)r[a]&&v(o,r[a]);if(t)if(n)for(i=i||b(e),r=r||b(s),a=0;null!=(o=i[a]);a++)y(o,r[a]);else y(e,s);return r=b(s,"script"),r.length>0&&m(r,!u&&b(e,"script")),r=i=o=null,s},buildFragment:function(e,t,n,r){for(var i,o,a,s,u,l,c,f=e.length,d=p(t),h=[],g=0;f>g;g++)if(o=e[g],o||0===o)if("object"===st.type(o))st.merge(h,o.nodeType?[o]:o);else if(Qt.test(o)){for(s=s||d.appendChild(t.createElement("div")),a=(Jt.exec(o)||["",""])[1].toLowerCase(),u=on[a]||on._default,s.innerHTML=u[1]+o.replace(Yt,"<$1></$2>")+u[2],c=u[0];c--;)s=s.lastChild;if(!st.support.leadingWhitespace&&Vt.test(o)&&h.push(t.createTextNode(Vt.exec(o)[0])),!st.support.tbody)for(o="table"!==a||Gt.test(o)?"<table>"!==u[1]||Gt.test(o)?0:s:s.firstChild,c=o&&o.childNodes.length;c--;)st.nodeName(l=o.childNodes[c],"tbody")&&!l.childNodes.length&&o.removeChild(l);for(st.merge(h,s.childNodes),s.textContent="";s.firstChild;)s.removeChild(s.firstChild);s=d.lastChild}else h.push(t.createTextNode(o));for(s&&d.removeChild(s),st.support.appendChecked||st.grep(b(h,"input"),x),g=0;o=h[g++];)if((!r||-1===st.inArray(o,r))&&(i=st.contains(o.ownerDocument,o),s=b(d.appendChild(o),"script"),i&&m(s),n))for(c=0;o=s[c++];)tn.test(o.type||"")&&n.push(o);return s=null,d},cleanData:function(e,n){for(var r,i,o,a,s=0,u=st.expando,l=st.cache,c=st.support.deleteExpando,f=st.event.special;null!=(o=e[s]);s++)if((n||st.acceptData(o))&&(i=o[u],r=i&&l[i])){if(r.events)for(a in r.events)f[a]?st.event.remove(o,a):st.removeEvent(o,a,r.handle);l[i]&&(delete l[i],c?delete o[u]:o.removeAttribute!==t?o.removeAttribute(u):o[u]=null,K.push(i))}}});var un,ln,cn,fn=/alpha\([^)]*\)/i,pn=/opacity\s*=\s*([^)]*)/,dn=/^(top|right|bottom|left)$/,hn=/^(none|table(?!-c[ea]).+)/,gn=/^margin/,mn=RegExp("^("+ut+")(.*)$","i"),yn=RegExp("^("+ut+")(?!px)[a-z%]+$","i"),vn=RegExp("^([+-])=("+ut+")","i"),bn={BODY:"block"},xn={position:"absolute",visibility:"hidden",display:"block"},Tn={letterSpacing:0,fontWeight:400},wn=["Top","Right","Bottom","Left"],Nn=["Webkit","O","Moz","ms"];st.fn.extend({css:function(e,n){return st.access(this,function(e,n,r){var i,o,a={},s=0;if(st.isArray(n)){for(i=ln(e),o=n.length;o>s;s++)a[n[s]]=st.css(e,n[s],!1,i);return a}return r!==t?st.style(e,n,r):st.css(e,n)},e,n,arguments.length>1)},show:function(){return N(this,!0)},hide:function(){return N(this)},toggle:function(e){var t="boolean"==typeof e;return this.each(function(){(t?e:w(this))?st(this).show():st(this).hide()})}}),st.extend({cssHooks:{opacity:{get:function(e,t){if(t){var n=un(e,"opacity");return""===n?"1":n}}}},cssNumber:{columnCount:!0,fillOpacity:!0,fontWeight:!0,lineHeight:!0,opacity:!0,orphans:!0,widows:!0,zIndex:!0,zoom:!0},cssProps:{"float":st.support.cssFloat?"cssFloat":"styleFloat"},style:function(e,n,r,i){if(e&&3!==e.nodeType&&8!==e.nodeType&&e.style){var o,a,s,u=st.camelCase(n),l=e.style;if(n=st.cssProps[u]||(st.cssProps[u]=T(l,u)),s=st.cssHooks[n]||st.cssHooks[u],r===t)return s&&"get"in s&&(o=s.get(e,!1,i))!==t?o:l[n];if(a=typeof r,"string"===a&&(o=vn.exec(r))&&(r=(o[1]+1)*o[2]+parseFloat(st.css(e,n)),a="number"),!(null==r||"number"===a&&isNaN(r)||("number"!==a||st.cssNumber[u]||(r+="px"),st.support.clearCloneStyle||""!==r||0!==n.indexOf("background")||(l[n]="inherit"),s&&"set"in s&&(r=s.set(e,r,i))===t)))try{l[n]=r}catch(c){}}},css:function(e,n,r,i){var o,a,s,u=st.camelCase(n);return n=st.cssProps[u]||(st.cssProps[u]=T(e.style,u)),s=st.cssHooks[n]||st.cssHooks[u],s&&"get"in s&&(o=s.get(e,!0,r)),o===t&&(o=un(e,n,i)),"normal"===o&&n in Tn&&(o=Tn[n]),r?(a=parseFloat(o),r===!0||st.isNumeric(a)?a||0:o):o},swap:function(e,t,n,r){var i,o,a={};for(o in t)a[o]=e.style[o],e.style[o]=t[o];i=n.apply(e,r||[]);for(o in t)e.style[o]=a[o];return i}}),e.getComputedStyle?(ln=function(t){return e.getComputedStyle(t,null)},un=function(e,n,r){var i,o,a,s=r||ln(e),u=s?s.getPropertyValue(n)||s[n]:t,l=e.style;return s&&(""!==u||st.contains(e.ownerDocument,e)||(u=st.style(e,n)),yn.test(u)&&gn.test(n)&&(i=l.width,o=l.minWidth,a=l.maxWidth,l.minWidth=l.maxWidth=l.width=u,u=s.width,l.width=i,l.minWidth=o,l.maxWidth=a)),u}):V.documentElement.currentStyle&&(ln=function(e){return e.currentStyle},un=function(e,n,r){var i,o,a,s=r||ln(e),u=s?s[n]:t,l=e.style;return null==u&&l&&l[n]&&(u=l[n]),yn.test(u)&&!dn.test(n)&&(i=l.left,o=e.runtimeStyle,a=o&&o.left,a&&(o.left=e.currentStyle.left),l.left="fontSize"===n?"1em":u,u=l.pixelLeft+"px",l.left=i,a&&(o.left=a)),""===u?"auto":u}),st.each(["height","width"],function(e,n){st.cssHooks[n]={get:function(e,r,i){return r?0===e.offsetWidth&&hn.test(st.css(e,"display"))?st.swap(e,xn,function(){return E(e,n,i)}):E(e,n,i):t},set:function(e,t,r){var i=r&&ln(e);return C(e,t,r?k(e,n,r,st.support.boxSizing&&"border-box"===st.css(e,"boxSizing",!1,i),i):0)}}}),st.support.opacity||(st.cssHooks.opacity={get:function(e,t){return pn.test((t&&e.currentStyle?e.currentStyle.filter:e.style.filter)||"")?.01*parseFloat(RegExp.$1)+"":t?"1":""},set:function(e,t){var n=e.style,r=e.currentStyle,i=st.isNumeric(t)?"alpha(opacity="+100*t+")":"",o=r&&r.filter||n.filter||"";n.zoom=1,(t>=1||""===t)&&""===st.trim(o.replace(fn,""))&&n.removeAttribute&&(n.removeAttribute("filter"),""===t||r&&!r.filter)||(n.filter=fn.test(o)?o.replace(fn,i):o+" "+i)}}),st(function(){st.support.reliableMarginRight||(st.cssHooks.marginRight={get:function(e,n){return n?st.swap(e,{display:"inline-block"},un,[e,"marginRight"]):t}}),!st.support.pixelPosition&&st.fn.position&&st.each(["top","left"],function(e,n){st.cssHooks[n]={get:function(e,r){return r?(r=un(e,n),yn.test(r)?st(e).position()[n]+"px":r):t}}})}),st.expr&&st.expr.filters&&(st.expr.filters.hidden=function(e){return 0===e.offsetWidth&&0===e.offsetHeight||!st.support.reliableHiddenOffsets&&"none"===(e.style&&e.style.display||st.css(e,"display"))},st.expr.filters.visible=function(e){return!st.expr.filters.hidden(e)}),st.each({margin:"",padding:"",border:"Width"},function(e,t){st.cssHooks[e+t]={expand:function(n){for(var r=0,i={},o="string"==typeof n?n.split(" "):[n];4>r;r++)i[e+wn[r]+t]=o[r]||o[r-2]||o[0];return i}},gn.test(e)||(st.cssHooks[e+t].set=C)});var Cn=/%20/g,kn=/\[\]$/,En=/\r?\n/g,Sn=/^(?:submit|button|image|reset)$/i,An=/^(?:input|select|textarea|keygen)/i;st.fn.extend({serialize:function(){return st.param(this.serializeArray())},serializeArray:function(){return this.map(function(){var e=st.prop(this,"elements");return e?st.makeArray(e):this}).filter(function(){var e=this.type;return this.name&&!st(this).is(":disabled")&&An.test(this.nodeName)&&!Sn.test(e)&&(this.checked||!Zt.test(e))}).map(function(e,t){var n=st(this).val();return null==n?null:st.isArray(n)?st.map(n,function(e){return{name:t.name,value:e.replace(En,"\r\n")}}):{name:t.name,value:n.replace(En,"\r\n")}}).get()}}),st.param=function(e,n){var r,i=[],o=function(e,t){t=st.isFunction(t)?t():null==t?"":t,i[i.length]=encodeURIComponent(e)+"="+encodeURIComponent(t)};if(n===t&&(n=st.ajaxSettings&&st.ajaxSettings.traditional),st.isArray(e)||e.jquery&&!st.isPlainObject(e))st.each(e,function(){o(this.name,this.value)});else for(r in e)j(r,e[r],n,o);return i.join("&").replace(Cn,"+")};var jn,Dn,Ln=st.now(),Hn=/\?/,Mn=/#.*$/,qn=/([?&])_=[^&]*/,_n=/^(.*?):[ \t]*([^\r\n]*)\r?$/gm,Fn=/^(?:about|app|app-storage|.+-extension|file|res|widget):$/,On=/^(?:GET|HEAD)$/,Bn=/^\/\//,Pn=/^([\w.+-]+:)(?:\/\/([^\/?#:]*)(?::(\d+)|)|)/,Rn=st.fn.load,Wn={},$n={},In="*/".concat("*");try{Dn=Y.href}catch(zn){Dn=V.createElement("a"),Dn.href="",Dn=Dn.href}jn=Pn.exec(Dn.toLowerCase())||[],st.fn.load=function(e,n,r){if("string"!=typeof e&&Rn)return Rn.apply(this,arguments);var i,o,a,s=this,u=e.indexOf(" ");return u>=0&&(i=e.slice(u,e.length),e=e.slice(0,u)),st.isFunction(n)?(r=n,n=t):n&&"object"==typeof n&&(o="POST"),s.length>0&&st.ajax({url:e,type:o,dataType:"html",data:n}).done(function(e){a=arguments,s.html(i?st("<div>").append(st.parseHTML(e)).find(i):e)}).complete(r&&function(e,t){s.each(r,a||[e.responseText,t,e])}),this},st.each(["ajaxStart","ajaxStop","ajaxComplete","ajaxError","ajaxSuccess","ajaxSend"],function(e,t){st.fn[t]=function(e){return this.on(t,e)}}),st.each(["get","post"],function(e,n){st[n]=function(e,r,i,o){return st.isFunction(r)&&(o=o||i,i=r,r=t),st.ajax({url:e,type:n,dataType:o,data:r,success:i})}}),st.extend({active:0,lastModified:{},etag:{},ajaxSettings:{url:Dn,type:"GET",isLocal:Fn.test(jn[1]),global:!0,processData:!0,async:!0,contentType:"application/x-www-form-urlencoded; charset=UTF-8",accepts:{"*":In,text:"text/plain",html:"text/html",xml:"application/xml, text/xml",json:"application/json, text/javascript"},contents:{xml:/xml/,html:/html/,json:/json/},responseFields:{xml:"responseXML",text:"responseText"},converters:{"* text":e.String,"text html":!0,"text json":st.parseJSON,"text xml":st.parseXML},flatOptions:{url:!0,context:!0}},ajaxSetup:function(e,t){return t?H(H(e,st.ajaxSettings),t):H(st.ajaxSettings,e)},ajaxPrefilter:D(Wn),ajaxTransport:D($n),ajax:function(e,n){function r(e,n,r,s){var l,f,v,b,T,N=n;2!==x&&(x=2,u&&clearTimeout(u),i=t,a=s||"",w.readyState=e>0?4:0,r&&(b=M(p,w,r)),e>=200&&300>e||304===e?(p.ifModified&&(T=w.getResponseHeader("Last-Modified"),T&&(st.lastModified[o]=T),T=w.getResponseHeader("etag"),T&&(st.etag[o]=T)),304===e?(l=!0,N="notmodified"):(l=q(p,b),N=l.state,f=l.data,v=l.error,l=!v)):(v=N,(e||!N)&&(N="error",0>e&&(e=0))),w.status=e,w.statusText=(n||N)+"",l?g.resolveWith(d,[f,N,w]):g.rejectWith(d,[w,N,v]),w.statusCode(y),y=t,c&&h.trigger(l?"ajaxSuccess":"ajaxError",[w,p,l?f:v]),m.fireWith(d,[w,N]),c&&(h.trigger("ajaxComplete",[w,p]),--st.active||st.event.trigger("ajaxStop")))}"object"==typeof e&&(n=e,e=t),n=n||{};var i,o,a,s,u,l,c,f,p=st.ajaxSetup({},n),d=p.context||p,h=p.context&&(d.nodeType||d.jquery)?st(d):st.event,g=st.Deferred(),m=st.Callbacks("once memory"),y=p.statusCode||{},v={},b={},x=0,T="canceled",w={readyState:0,getResponseHeader:function(e){var t;if(2===x){if(!s)for(s={};t=_n.exec(a);)s[t[1].toLowerCase()]=t[2];t=s[e.toLowerCase()]}return null==t?null:t},getAllResponseHeaders:function(){return 2===x?a:null},setRequestHeader:function(e,t){var n=e.toLowerCase();return x||(e=b[n]=b[n]||e,v[e]=t),this},overrideMimeType:function(e){return x||(p.mimeType=e),this},statusCode:function(e){var t;if(e)if(2>x)for(t in e)y[t]=[y[t],e[t]];else w.always(e[w.status]);return this},abort:function(e){var t=e||T;return i&&i.abort(t),r(0,t),this}};if(g.promise(w).complete=m.add,w.success=w.done,w.error=w.fail,p.url=((e||p.url||Dn)+"").replace(Mn,"").replace(Bn,jn[1]+"//"),p.type=n.method||n.type||p.method||p.type,p.dataTypes=st.trim(p.dataType||"*").toLowerCase().match(lt)||[""],null==p.crossDomain&&(l=Pn.exec(p.url.toLowerCase()),p.crossDomain=!(!l||l[1]===jn[1]&&l[2]===jn[2]&&(l[3]||("http:"===l[1]?80:443))==(jn[3]||("http:"===jn[1]?80:443)))),p.data&&p.processData&&"string"!=typeof p.data&&(p.data=st.param(p.data,p.traditional)),L(Wn,p,n,w),2===x)return w;c=p.global,c&&0===st.active++&&st.event.trigger("ajaxStart"),p.type=p.type.toUpperCase(),p.hasContent=!On.test(p.type),o=p.url,p.hasContent||(p.data&&(o=p.url+=(Hn.test(o)?"&":"?")+p.data,delete p.data),p.cache===!1&&(p.url=qn.test(o)?o.replace(qn,"$1_="+Ln++):o+(Hn.test(o)?"&":"?")+"_="+Ln++)),p.ifModified&&(st.lastModified[o]&&w.setRequestHeader("If-Modified-Since",st.lastModified[o]),st.etag[o]&&w.setRequestHeader("If-None-Match",st.etag[o])),(p.data&&p.hasContent&&p.contentType!==!1||n.contentType)&&w.setRequestHeader("Content-Type",p.contentType),w.setRequestHeader("Accept",p.dataTypes[0]&&p.accepts[p.dataTypes[0]]?p.accepts[p.dataTypes[0]]+("*"!==p.dataTypes[0]?", "+In+"; q=0.01":""):p.accepts["*"]);for(f in p.headers)w.setRequestHeader(f,p.headers[f]);if(p.beforeSend&&(p.beforeSend.call(d,w,p)===!1||2===x))return w.abort();T="abort";for(f in{success:1,error:1,complete:1})w[f](p[f]);if(i=L($n,p,n,w)){w.readyState=1,c&&h.trigger("ajaxSend",[w,p]),p.async&&p.timeout>0&&(u=setTimeout(function(){w.abort("timeout")},p.timeout));try{x=1,i.send(v,r)}catch(N){if(!(2>x))throw N;r(-1,N)}}else r(-1,"No Transport");return w},getScript:function(e,n){return st.get(e,t,n,"script")},getJSON:function(e,t,n){return st.get(e,t,n,"json")}}),st.ajaxSetup({accepts:{script:"text/javascript, application/javascript, application/ecmascript, application/x-ecmascript"},contents:{script:/(?:java|ecma)script/},converters:{"text script":function(e){return st.globalEval(e),e}}}),st.ajaxPrefilter("script",function(e){e.cache===t&&(e.cache=!1),e.crossDomain&&(e.type="GET",e.global=!1)}),st.ajaxTransport("script",function(e){if(e.crossDomain){var n,r=V.head||st("head")[0]||V.documentElement;return{send:function(t,i){n=V.createElement("script"),n.async=!0,e.scriptCharset&&(n.charset=e.scriptCharset),n.src=e.url,n.onload=n.onreadystatechange=function(e,t){(t||!n.readyState||/loaded|complete/.test(n.readyState))&&(n.onload=n.onreadystatechange=null,n.parentNode&&n.parentNode.removeChild(n),n=null,t||i(200,"success"))},r.insertBefore(n,r.firstChild)},abort:function(){n&&n.onload(t,!0)}}}});var Xn=[],Un=/(=)\?(?=&|$)|\?\?/;st.ajaxSetup({jsonp:"callback",jsonpCallback:function(){var e=Xn.pop()||st.expando+"_"+Ln++;return this[e]=!0,e}}),st.ajaxPrefilter("json jsonp",function(n,r,i){var o,a,s,u=n.jsonp!==!1&&(Un.test(n.url)?"url":"string"==typeof n.data&&!(n.contentType||"").indexOf("application/x-www-form-urlencoded")&&Un.test(n.data)&&"data");return u||"jsonp"===n.dataTypes[0]?(o=n.jsonpCallback=st.isFunction(n.jsonpCallback)?n.jsonpCallback():n.jsonpCallback,u?n[u]=n[u].replace(Un,"$1"+o):n.jsonp!==!1&&(n.url+=(Hn.test(n.url)?"&":"?")+n.jsonp+"="+o),n.converters["script json"]=function(){return s||st.error(o+" was not called"),s[0]},n.dataTypes[0]="json",a=e[o],e[o]=function(){s=arguments},i.always(function(){e[o]=a,n[o]&&(n.jsonpCallback=r.jsonpCallback,Xn.push(o)),s&&st.isFunction(a)&&a(s[0]),s=a=t}),"script"):t});var Vn,Yn,Jn=0,Gn=e.ActiveXObject&&function(){var e;for(e in Vn)Vn[e](t,!0)};st.ajaxSettings.xhr=e.ActiveXObject?function(){return!this.isLocal&&_()||F()}:_,Yn=st.ajaxSettings.xhr(),st.support.cors=!!Yn&&"withCredentials"in Yn,Yn=st.support.ajax=!!Yn,Yn&&st.ajaxTransport(function(n){if(!n.crossDomain||st.support.cors){var r;return{send:function(i,o){var a,s,u=n.xhr();if(n.username?u.open(n.type,n.url,n.async,n.username,n.password):u.open(n.type,n.url,n.async),n.xhrFields)for(s in n.xhrFields)u[s]=n.xhrFields[s];n.mimeType&&u.overrideMimeType&&u.overrideMimeType(n.mimeType),n.crossDomain||i["X-Requested-With"]||(i["X-Requested-With"]="XMLHttpRequest");try{for(s in i)u.setRequestHeader(s,i[s])}catch(l){}u.send(n.hasContent&&n.data||null),r=function(e,i){var s,l,c,f,p;try{if(r&&(i||4===u.readyState))if(r=t,a&&(u.onreadystatechange=st.noop,Gn&&delete Vn[a]),i)4!==u.readyState&&u.abort();else{f={},s=u.status,p=u.responseXML,c=u.getAllResponseHeaders(),p&&p.documentElement&&(f.xml=p),"string"==typeof u.responseText&&(f.text=u.responseText);try{l=u.statusText}catch(d){l=""}s||!n.isLocal||n.crossDomain?1223===s&&(s=204):s=f.text?200:404}}catch(h){i||o(-1,h)}f&&o(s,l,f,c)},n.async?4===u.readyState?setTimeout(r):(a=++Jn,Gn&&(Vn||(Vn={},st(e).unload(Gn)),Vn[a]=r),u.onreadystatechange=r):r()},abort:function(){r&&r(t,!0)}}}});var Qn,Kn,Zn=/^(?:toggle|show|hide)$/,er=RegExp("^(?:([+-])=|)("+ut+")([a-z%]*)$","i"),tr=/queueHooks$/,nr=[W],rr={"*":[function(e,t){var n,r,i=this.createTween(e,t),o=er.exec(t),a=i.cur(),s=+a||0,u=1,l=20;if(o){if(n=+o[2],r=o[3]||(st.cssNumber[e]?"":"px"),"px"!==r&&s){s=st.css(i.elem,e,!0)||n||1;do u=u||".5",s/=u,st.style(i.elem,e,s+r);while(u!==(u=i.cur()/a)&&1!==u&&--l)}i.unit=r,i.start=s,i.end=o[1]?s+(o[1]+1)*n:n}return i}]};st.Animation=st.extend(P,{tweener:function(e,t){st.isFunction(e)?(t=e,e=["*"]):e=e.split(" ");for(var n,r=0,i=e.length;i>r;r++)n=e[r],rr[n]=rr[n]||[],rr[n].unshift(t)},prefilter:function(e,t){t?nr.unshift(e):nr.push(e)}}),st.Tween=$,$.prototype={constructor:$,init:function(e,t,n,r,i,o){this.elem=e,this.prop=n,this.easing=i||"swing",this.options=t,this.start=this.now=this.cur(),this.end=r,this.unit=o||(st.cssNumber[n]?"":"px")},cur:function(){var e=$.propHooks[this.prop];return e&&e.get?e.get(this):$.propHooks._default.get(this)},run:function(e){var t,n=$.propHooks[this.prop];return this.pos=t=this.options.duration?st.easing[this.easing](e,this.options.duration*e,0,1,this.options.duration):e,this.now=(this.end-this.start)*t+this.start,this.options.step&&this.options.step.call(this.elem,this.now,this),n&&n.set?n.set(this):$.propHooks._default.set(this),this}},$.prototype.init.prototype=$.prototype,$.propHooks={_default:{get:function(e){var t;return null==e.elem[e.prop]||e.elem.style&&null!=e.elem.style[e.prop]?(t=st.css(e.elem,e.prop,"auto"),t&&"auto"!==t?t:0):e.elem[e.prop]},set:function(e){st.fx.step[e.prop]?st.fx.step[e.prop](e):e.elem.style&&(null!=e.elem.style[st.cssProps[e.prop]]||st.cssHooks[e.prop])?st.style(e.elem,e.prop,e.now+e.unit):e.elem[e.prop]=e.now}}},$.propHooks.scrollTop=$.propHooks.scrollLeft={set:function(e){e.elem.nodeType&&e.elem.parentNode&&(e.elem[e.prop]=e.now)}},st.each(["toggle","show","hide"],function(e,t){var n=st.fn[t];st.fn[t]=function(e,r,i){return null==e||"boolean"==typeof e?n.apply(this,arguments):this.animate(I(t,!0),e,r,i)}}),st.fn.extend({fadeTo:function(e,t,n,r){return this.filter(w).css("opacity",0).show().end().animate({opacity:t},e,n,r)},animate:function(e,t,n,r){var i=st.isEmptyObject(e),o=st.speed(t,n,r),a=function(){var t=P(this,st.extend({},e),o);a.finish=function(){t.stop(!0)},(i||st._data(this,"finish"))&&t.stop(!0)};return a.finish=a,i||o.queue===!1?this.each(a):this.queue(o.queue,a)},stop:function(e,n,r){var i=function(e){var t=e.stop;delete e.stop,t(r)};return"string"!=typeof e&&(r=n,n=e,e=t),n&&e!==!1&&this.queue(e||"fx",[]),this.each(function(){var t=!0,n=null!=e&&e+"queueHooks",o=st.timers,a=st._data(this);if(n)a[n]&&a[n].stop&&i(a[n]);else for(n in a)a[n]&&a[n].stop&&tr.test(n)&&i(a[n]);for(n=o.length;n--;)o[n].elem!==this||null!=e&&o[n].queue!==e||(o[n].anim.stop(r),t=!1,o.splice(n,1));(t||!r)&&st.dequeue(this,e)})},finish:function(e){return e!==!1&&(e=e||"fx"),this.each(function(){var t,n=st._data(this),r=n[e+"queue"],i=n[e+"queueHooks"],o=st.timers,a=r?r.length:0;for(n.finish=!0,st.queue(this,e,[]),i&&i.cur&&i.cur.finish&&i.cur.finish.call(this),t=o.length;t--;)o[t].elem===this&&o[t].queue===e&&(o[t].anim.stop(!0),o.splice(t,1));for(t=0;a>t;t++)r[t]&&r[t].finish&&r[t].finish.call(this);delete n.finish})}}),st.each({slideDown:I("show"),slideUp:I("hide"),slideToggle:I("toggle"),fadeIn:{opacity:"show"},fadeOut:{opacity:"hide"},fadeToggle:{opacity:"toggle"}},function(e,t){st.fn[e]=function(e,n,r){return this.animate(t,e,n,r)}}),st.speed=function(e,t,n){var r=e&&"object"==typeof e?st.extend({},e):{complete:n||!n&&t||st.isFunction(e)&&e,duration:e,easing:n&&t||t&&!st.isFunction(t)&&t};return r.duration=st.fx.off?0:"number"==typeof r.duration?r.duration:r.duration in st.fx.speeds?st.fx.speeds[r.duration]:st.fx.speeds._default,(null==r.queue||r.queue===!0)&&(r.queue="fx"),r.old=r.complete,r.complete=function(){st.isFunction(r.old)&&r.old.call(this),r.queue&&st.dequeue(this,r.queue)},r},st.easing={linear:function(e){return e},swing:function(e){return.5-Math.cos(e*Math.PI)/2}},st.timers=[],st.fx=$.prototype.init,st.fx.tick=function(){var e,n=st.timers,r=0;for(Qn=st.now();n.length>r;r++)e=n[r],e()||n[r]!==e||n.splice(r--,1);n.length||st.fx.stop(),Qn=t},st.fx.timer=function(e){e()&&st.timers.push(e)&&st.fx.start()},st.fx.interval=13,st.fx.start=function(){Kn||(Kn=setInterval(st.fx.tick,st.fx.interval))},st.fx.stop=function(){clearInterval(Kn),Kn=null},st.fx.speeds={slow:600,fast:200,_default:400},st.fx.step={},st.expr&&st.expr.filters&&(st.expr.filters.animated=function(e){return st.grep(st.timers,function(t){return e===t.elem}).length}),st.fn.offset=function(e){if(arguments.length)return e===t?this:this.each(function(t){st.offset.setOffset(this,e,t)});var n,r,i={top:0,left:0},o=this[0],a=o&&o.ownerDocument;if(a)return n=a.documentElement,st.contains(n,o)?(o.getBoundingClientRect!==t&&(i=o.getBoundingClientRect()),r=z(a),{top:i.top+(r.pageYOffset||n.scrollTop)-(n.clientTop||0),left:i.left+(r.pageXOffset||n.scrollLeft)-(n.clientLeft||0)}):i},st.offset={setOffset:function(e,t,n){var r=st.css(e,"position");"static"===r&&(e.style.position="relative");var i,o,a=st(e),s=a.offset(),u=st.css(e,"top"),l=st.css(e,"left"),c=("absolute"===r||"fixed"===r)&&st.inArray("auto",[u,l])>-1,f={},p={};c?(p=a.position(),i=p.top,o=p.left):(i=parseFloat(u)||0,o=parseFloat(l)||0),st.isFunction(t)&&(t=t.call(e,n,s)),null!=t.top&&(f.top=t.top-s.top+i),null!=t.left&&(f.left=t.left-s.left+o),"using"in t?t.using.call(e,f):a.css(f)}},st.fn.extend({position:function(){if(this[0]){var e,t,n={top:0,left:0},r=this[0];return"fixed"===st.css(r,"position")?t=r.getBoundingClientRect():(e=this.offsetParent(),t=this.offset(),st.nodeName(e[0],"html")||(n=e.offset()),n.top+=st.css(e[0],"borderTopWidth",!0),n.left+=st.css(e[0],"borderLeftWidth",!0)),{top:t.top-n.top-st.css(r,"marginTop",!0),left:t.left-n.left-st.css(r,"marginLeft",!0)}}},offsetParent:function(){return this.map(function(){for(var e=this.offsetParent||V.documentElement;e&&!st.nodeName(e,"html")&&"static"===st.css(e,"position");)e=e.offsetParent;return e||V.documentElement})}}),st.each({scrollLeft:"pageXOffset",scrollTop:"pageYOffset"},function(e,n){var r=/Y/.test(n);st.fn[e]=function(i){return st.access(this,function(e,i,o){var a=z(e);return o===t?a?n in a?a[n]:a.document.documentElement[i]:e[i]:(a?a.scrollTo(r?st(a).scrollLeft():o,r?o:st(a).scrollTop()):e[i]=o,t)},e,i,arguments.length,null)}}),st.each({Height:"height",Width:"width"},function(e,n){st.each({padding:"inner"+e,content:n,"":"outer"+e},function(r,i){st.fn[i]=function(i,o){var a=arguments.length&&(r||"boolean"!=typeof i),s=r||(i===!0||o===!0?"margin":"border");return st.access(this,function(n,r,i){var o;return st.isWindow(n)?n.document.documentElement["client"+e]:9===n.nodeType?(o=n.documentElement,Math.max(n.body["scroll"+e],o["scroll"+e],n.body["offset"+e],o["offset"+e],o["client"+e])):i===t?st.css(n,r,s):st.style(n,r,i,s)},n,a?i:t,a,null)}})}),e.jQuery=e.$=st,"function"==typeof define&&define.amd&&define.amd.jQuery&&define("jquery",[],function(){return st})})(window);
//@ sourceMappingURL=jquery.min.map
/** javascripts/vendor/jquery.colorbox.js **/
/*!
	Colorbox v1.5.9 - 2014-04-25
	jQuery lightbox and modal window plugin
	(c) 2014 Jack Moore - http://www.jacklmoore.com/colorbox
	license: http://www.opensource.org/licenses/mit-license.php
*/
(function ($, document, window) {
	var
	// Default settings object.
	// See http://jacklmoore.com/colorbox for details.
	defaults = {
		// data sources
		html: false,
		photo: false,
		iframe: false,
		inline: false,

		// behavior and appearance
		transition: "elastic",
		speed: 300,
		fadeOut: 300,
		width: false,
		initialWidth: "600",
		innerWidth: false,
		maxWidth: false,
		height: false,
		initialHeight: "450",
		innerHeight: false,
		maxHeight: false,
		scalePhotos: true,
		scrolling: true,
		opacity: 0.9,
		preloading: true,
		className: false,
		overlayClose: true,
		escKey: true,
		arrowKey: true,
		top: false,
		bottom: false,
		left: false,
		right: false,
		fixed: false,
		data: undefined,
		closeButton: true,
		fastIframe: true,
		open: false,
		reposition: true,
		loop: true,
		slideshow: false,
		slideshowAuto: true,
		slideshowSpeed: 2500,
		slideshowStart: "start slideshow",
		slideshowStop: "stop slideshow",
		photoRegex: /\.(gif|png|jp(e|g|eg)|bmp|ico|webp|jxr|svg)((#|\?).*)?$/i,

		// alternate image paths for high-res displays
		retinaImage: false,
		retinaUrl: false,
		retinaSuffix: '@2x.$1',

		// internationalization
		current: "image {current} of {total}",
		previous: "previous",
		next: "next",
		close: "close",
		xhrError: "This content failed to load.",
		imgError: "This image failed to load.",

		// accessbility
		returnFocus: true,
		trapFocus: true,

		// callbacks
		onOpen: false,
		onLoad: false,
		onComplete: false,
		onCleanup: false,
		onClosed: false,

		rel: function() {
			return this.rel;
		},
		href: function() {
			// using this.href would give the absolute url, when the href may have been inteded as a selector (e.g. '#container')
			return $(this).attr('href');
		},
		title: function() {
			return this.title;
		}
	},


	// Abstracting the HTML and event identifiers for easy rebranding
	colorbox = 'colorbox',
	prefix = 'cbox',
	boxElement = prefix + 'Element',
	
	// Events
	event_open = prefix + '_open',
	event_load = prefix + '_load',
	event_complete = prefix + '_complete',
	event_cleanup = prefix + '_cleanup',
	event_closed = prefix + '_closed',
	event_purge = prefix + '_purge',

	// Cached jQuery Object Variables
	$overlay,
	$box,
	$wrap,
	$content,
	$topBorder,
	$leftBorder,
	$rightBorder,
	$bottomBorder,
	$related,
	$window,
	$loaded,
	$loadingBay,
	$loadingOverlay,
	$title,
	$current,
	$slideshow,
	$next,
	$prev,
	$close,
	$groupControls,
	$events = $('<a/>'), // $({}) would be prefered, but there is an issue with jQuery 1.4.2
	
	// Variables for cached values or use across multiple functions
	settings,
	interfaceHeight,
	interfaceWidth,
	loadedHeight,
	loadedWidth,
	index,
	photo,
	open,
	active,
	closing,
	loadingTimer,
	publicMethod,
	div = "div",
	requests = 0,
	previousCSS = {},
	init;

	// ****************
	// HELPER FUNCTIONS
	// ****************
	
	// Convenience function for creating new jQuery objects
	function $tag(tag, id, css) {
		var element = document.createElement(tag);

		if (id) {
			element.id = prefix + id;
		}

		if (css) {
			element.style.cssText = css;
		}

		return $(element);
	}
	
	// Get the window height using innerHeight when available to avoid an issue with iOS
	// http://bugs.jquery.com/ticket/6724
	function winheight() {
		return window.innerHeight ? window.innerHeight : $(window).height();
	}

	function Settings(element, options) {
		if (options !== Object(options)) {
			options = {};
		}

		this.cache = {};
		this.el = element;

		this.value = function(key) {
			var dataAttr;

			if (this.cache[key] === undefined) {
				dataAttr = $(this.el).attr('data-cbox-'+key);

				if (dataAttr !== undefined) {
					this.cache[key] = dataAttr;
				} else if (options[key] !== undefined) {
					this.cache[key] = options[key];
				} else if (defaults[key] !== undefined) {
					this.cache[key] = defaults[key];
				}
			}

			return this.cache[key];
		};

		this.get = function(key) {
			var value = this.value(key);
			return $.isFunction(value) ? value.call(this.el, this) : value;
		};
	}

	// Determine the next and previous members in a group.
	function getIndex(increment) {
		var
		max = $related.length,
		newIndex = (index + increment) % max;
		
		return (newIndex < 0) ? max + newIndex : newIndex;
	}

	// Convert '%' and 'px' values to integers
	function setSize(size, dimension) {
		return Math.round((/%/.test(size) ? ((dimension === 'x' ? $window.width() : winheight()) / 100) : 1) * parseInt(size, 10));
	}
	
	// Checks an href to see if it is a photo.
	// There is a force photo option (photo: true) for hrefs that cannot be matched by the regex.
	function isImage(settings, url) {
		return settings.get('photo') || settings.get('photoRegex').test(url);
	}

	function retinaUrl(settings, url) {
		return settings.get('retinaUrl') && window.devicePixelRatio > 1 ? url.replace(settings.get('photoRegex'), settings.get('retinaSuffix')) : url;
	}

	function trapFocus(e) {
		if ('contains' in $box[0] && !$box[0].contains(e.target) && e.target !== $overlay[0]) {
			e.stopPropagation();
			$box.focus();
		}
	}

	function setClass(str) {
		if (setClass.str !== str) {
			$box.add($overlay).removeClass(setClass.str).addClass(str);
			setClass.str = str;
		}
	}

	function getRelated(rel) {
		index = 0;
		
		if (rel && rel !== false && rel !== 'nofollow') {
			$related = $('.' + boxElement).filter(function () {
				var options = $.data(this, colorbox);
				var settings = new Settings(this, options);
				return (settings.get('rel') === rel);
			});
			index = $related.index(settings.el);
			
			// Check direct calls to Colorbox.
			if (index === -1) {
				$related = $related.add(settings.el);
				index = $related.length - 1;
			}
		} else {
			$related = $(settings.el);
		}
	}

	function trigger(event) {
		// for external use
		$(document).trigger(event);
		// for internal use
		$events.triggerHandler(event);
	}

	var slideshow = (function(){
		var active,
			className = prefix + "Slideshow_",
			click = "click." + prefix,
			timeOut;

		function clear () {
			clearTimeout(timeOut);
		}

		function set() {
			if (settings.get('loop') || $related[index + 1]) {
				clear();
				timeOut = setTimeout(publicMethod.next, settings.get('slideshowSpeed'));
			}
		}

		function start() {
			$slideshow
				.html(settings.get('slideshowStop'))
				.unbind(click)
				.one(click, stop);

			$events
				.bind(event_complete, set)
				.bind(event_load, clear);

			$box.removeClass(className + "off").addClass(className + "on");
		}

		function stop() {
			clear();
			
			$events
				.unbind(event_complete, set)
				.unbind(event_load, clear);

			$slideshow
				.html(settings.get('slideshowStart'))
				.unbind(click)
				.one(click, function () {
					publicMethod.next();
					start();
				});

			$box.removeClass(className + "on").addClass(className + "off");
		}

		function reset() {
			active = false;
			$slideshow.hide();
			clear();
			$events
				.unbind(event_complete, set)
				.unbind(event_load, clear);
			$box.removeClass(className + "off " + className + "on");
		}

		return function(){
			if (active) {
				if (!settings.get('slideshow')) {
					$events.unbind(event_cleanup, reset);
					reset();
				}
			} else {
				if (settings.get('slideshow') && $related[1]) {
					active = true;
					$events.one(event_cleanup, reset);
					if (settings.get('slideshowAuto')) {
						start();
					} else {
						stop();
					}
					$slideshow.show();
				}
			}
		};

	}());


	function launch(element) {
		var options;

		if (!closing) {

			options = $(element).data('colorbox');

			settings = new Settings(element, options);
			
			getRelated(settings.get('rel'));

			if (!open) {
				open = active = true; // Prevents the page-change action from queuing up if the visitor holds down the left or right keys.

				setClass(settings.get('className'));
				
				// Show colorbox so the sizes can be calculated in older versions of jQuery
				$box.css({visibility:'hidden', display:'block', opacity:''});
				
				$loaded = $tag(div, 'LoadedContent', 'width:0; height:0; overflow:hidden; visibility:hidden');
				$content.css({width:'', height:''}).append($loaded);

				// Cache values needed for size calculations
				interfaceHeight = $topBorder.height() + $bottomBorder.height() + $content.outerHeight(true) - $content.height();
				interfaceWidth = $leftBorder.width() + $rightBorder.width() + $content.outerWidth(true) - $content.width();
				loadedHeight = $loaded.outerHeight(true);
				loadedWidth = $loaded.outerWidth(true);

				// Opens inital empty Colorbox prior to content being loaded.
				var initialWidth = setSize(settings.get('initialWidth'), 'x');
				var initialHeight = setSize(settings.get('initialHeight'), 'y');
				var maxWidth = settings.get('maxWidth');
				var maxHeight = settings.get('maxHeight');

				settings.w = (maxWidth !== false ? Math.min(initialWidth, setSize(maxWidth, 'x')) : initialWidth) - loadedWidth - interfaceWidth;
				settings.h = (maxHeight !== false ? Math.min(initialHeight, setSize(maxHeight, 'y')) : initialHeight) - loadedHeight - interfaceHeight;

				$loaded.css({width:'', height:settings.h});
				publicMethod.position();

				trigger(event_open);
				settings.get('onOpen');

				$groupControls.add($title).hide();

				$box.focus();
				
				if (settings.get('trapFocus')) {
					// Confine focus to the modal
					// Uses event capturing that is not supported in IE8-
					if (document.addEventListener) {

						document.addEventListener('focus', trapFocus, true);
						
						$events.one(event_closed, function () {
							document.removeEventListener('focus', trapFocus, true);
						});
					}
				}

				// Return focus on closing
				if (settings.get('returnFocus')) {
					$events.one(event_closed, function () {
						$(settings.el).focus();
					});
				}
			}

			$overlay.css({
				opacity: parseFloat(settings.get('opacity')) || '',
				cursor: settings.get('overlayClose') ? 'pointer' : '',
				visibility: 'visible'
			}).show();
			
			if (settings.get('closeButton')) {
				$close.html(settings.get('close')).appendTo($content);
			} else {
				$close.appendTo('<div/>'); // replace with .detach() when dropping jQuery < 1.4
			}

			load();
		}
	}

	// Colorbox's markup needs to be added to the DOM prior to being called
	// so that the browser will go ahead and load the CSS background images.
	function appendHTML() {
		if (!$box && document.body) {
			init = false;
			$window = $(window);
			$box = $tag(div).attr({
				id: colorbox,
				'class': $.support.opacity === false ? prefix + 'IE' : '', // class for optional IE8 & lower targeted CSS.
				role: 'dialog',
				tabindex: '-1'
			}).hide();
			$overlay = $tag(div, "Overlay").hide();
			$loadingOverlay = $([$tag(div, "LoadingOverlay")[0],$tag(div, "LoadingGraphic")[0]]);
			$wrap = $tag(div, "Wrapper");
			$content = $tag(div, "Content").append(
				$title = $tag(div, "Title"),
				$current = $tag(div, "Current"),
				$prev = $('<button type="button" aria-label="previous"/>').attr({id:prefix+'Previous'}),
				$next = $('<button type="button" aria-label="next"/>').attr({id:prefix+'Next'}),
				$slideshow = $('<button type="button" aria-label="slideshow"/>').attr({id:prefix+'Slideshow'}),
				$loadingOverlay
			);

			$close = $('<button type="button" aria-label="close"/>').attr({id:prefix+'Close'});
			
			$wrap.append( // The 3x3 Grid that makes up Colorbox
				$tag(div).append(
					$tag(div, "TopLeft"),
					$topBorder = $tag(div, "TopCenter"),
					$tag(div, "TopRight")
				),
				$tag(div, false, 'clear:left').append(
					$leftBorder = $tag(div, "MiddleLeft"),
					$content,
					$rightBorder = $tag(div, "MiddleRight")
				),
				$tag(div, false, 'clear:left').append(
					$tag(div, "BottomLeft"),
					$bottomBorder = $tag(div, "BottomCenter"),
					$tag(div, "BottomRight")
				)
			).find('div div').css({'float': 'left'});
			
			$loadingBay = $tag(div, false, 'position:absolute; width:9999px; visibility:hidden; display:none; max-width:none;');
			
			$groupControls = $next.add($prev).add($current).add($slideshow);

			$(document.body).append($overlay, $box.append($wrap, $loadingBay));
		}
	}

	// Add Colorbox's event bindings
	function addBindings() {
		function clickHandler(e) {
			// ignore non-left-mouse-clicks and clicks modified with ctrl / command, shift, or alt.
			// See: http://jacklmoore.com/notes/click-events/
			if (!(e.which > 1 || e.shiftKey || e.altKey || e.metaKey || e.ctrlKey)) {
				e.preventDefault();
				launch(this);
			}
		}

		if ($box) {
			if (!init) {
				init = true;

				// Anonymous functions here keep the public method from being cached, thereby allowing them to be redefined on the fly.
				$next.click(function () {
					publicMethod.next();
				});
				$prev.click(function () {
					publicMethod.prev();
				});
				$close.click(function () {
					publicMethod.close();
				});
				$overlay.click(function () {
					if (settings.get('overlayClose')) {
						publicMethod.close();
					}
				});
				
				// Key Bindings
				$(document).bind('keydown.' + prefix, function (e) {
					var key = e.keyCode;
					if (open && settings.get('escKey') && key === 27) {
						e.preventDefault();
						publicMethod.close();
					}
					if (open && settings.get('arrowKey') && $related[1] && !e.altKey) {
						if (key === 37) {
							e.preventDefault();
							$prev.click();
						} else if (key === 39) {
							e.preventDefault();
							$next.click();
						}
					}
				});

				if ($.isFunction($.fn.on)) {
					// For jQuery 1.7+
					$(document).on('click.'+prefix, '.'+boxElement, clickHandler);
				} else {
					// For jQuery 1.3.x -> 1.6.x
					// This code is never reached in jQuery 1.9, so do not contact me about 'live' being removed.
					// This is not here for jQuery 1.9, it's here for legacy users.
					$('.'+boxElement).live('click.'+prefix, clickHandler);
				}
			}
			return true;
		}
		return false;
	}

	// Don't do anything if Colorbox already exists.
	if ($.colorbox) {
		return;
	}

	// Append the HTML when the DOM loads
	$(appendHTML);


	// ****************
	// PUBLIC FUNCTIONS
	// Usage format: $.colorbox.close();
	// Usage from within an iframe: parent.jQuery.colorbox.close();
	// ****************
	
	publicMethod = $.fn[colorbox] = $[colorbox] = function (options, callback) {
		var settings;
		var $obj = this;

		options = options || {};

		if ($.isFunction($obj)) { // assume a call to $.colorbox
			$obj = $('<a/>');
			options.open = true;
		} else if (!$obj[0]) { // colorbox being applied to empty collection
			return $obj;
		}


		if (!$obj[0]) { // colorbox being applied to empty collection
			return $obj;
		}
		
		appendHTML();

		if (addBindings()) {

			if (callback) {
				options.onComplete = callback;
			}

			$obj.each(function () {
				var old = $.data(this, colorbox) || {};
				$.data(this, colorbox, $.extend(old, options));
			}).addClass(boxElement);

			settings = new Settings($obj[0], options);
			
			if (settings.get('open')) {
				launch($obj[0]);
			}
		}
		
		return $obj;
	};

	publicMethod.position = function (speed, loadedCallback) {
		var
		css,
		top = 0,
		left = 0,
		offset = $box.offset(),
		scrollTop,
		scrollLeft;
		
		$window.unbind('resize.' + prefix);

		// remove the modal so that it doesn't influence the document width/height
		$box.css({top: -9e4, left: -9e4});

		scrollTop = $window.scrollTop();
		scrollLeft = $window.scrollLeft();

		if (settings.get('fixed')) {
			offset.top -= scrollTop;
			offset.left -= scrollLeft;
			$box.css({position: 'fixed'});
		} else {
			top = scrollTop;
			left = scrollLeft;
			$box.css({position: 'absolute'});
		}

		// keeps the top and left positions within the browser's viewport.
		if (settings.get('right') !== false) {
			left += Math.max($window.width() - settings.w - loadedWidth - interfaceWidth - setSize(settings.get('right'), 'x'), 0);
		} else if (settings.get('left') !== false) {
			left += setSize(settings.get('left'), 'x');
		} else {
			left += Math.round(Math.max($window.width() - settings.w - loadedWidth - interfaceWidth, 0) / 2);
		}
		
		if (settings.get('bottom') !== false) {
			top += Math.max(winheight() - settings.h - loadedHeight - interfaceHeight - setSize(settings.get('bottom'), 'y'), 0);
		} else if (settings.get('top') !== false) {
			top += setSize(settings.get('top'), 'y');
		} else {
			top += Math.round(Math.max(winheight() - settings.h - loadedHeight - interfaceHeight, 0) / 2);
		}

		$box.css({top: offset.top, left: offset.left, visibility:'visible'});
		
		// this gives the wrapper plenty of breathing room so it's floated contents can move around smoothly,
		// but it has to be shrank down around the size of div#colorbox when it's done.  If not,
		// it can invoke an obscure IE bug when using iframes.
		$wrap[0].style.width = $wrap[0].style.height = "9999px";
		
		function modalDimensions() {
			$topBorder[0].style.width = $bottomBorder[0].style.width = $content[0].style.width = (parseInt($box[0].style.width,10) - interfaceWidth)+'px';
			$content[0].style.height = $leftBorder[0].style.height = $rightBorder[0].style.height = (parseInt($box[0].style.height,10) - interfaceHeight)+'px';
		}

		css = {width: settings.w + loadedWidth + interfaceWidth, height: settings.h + loadedHeight + interfaceHeight, top: top, left: left};

		// setting the speed to 0 if the content hasn't changed size or position
		if (speed) {
			var tempSpeed = 0;
			$.each(css, function(i){
				if (css[i] !== previousCSS[i]) {
					tempSpeed = speed;
					return;
				}
			});
			speed = tempSpeed;
		}

		previousCSS = css;

		if (!speed) {
			$box.css(css);
		}

		$box.dequeue().animate(css, {
			duration: speed || 0,
			complete: function () {
				modalDimensions();
				
				active = false;
				
				// shrink the wrapper down to exactly the size of colorbox to avoid a bug in IE's iframe implementation.
				$wrap[0].style.width = (settings.w + loadedWidth + interfaceWidth) + "px";
				$wrap[0].style.height = (settings.h + loadedHeight + interfaceHeight) + "px";
				
				if (settings.get('reposition')) {
					setTimeout(function () {  // small delay before binding onresize due to an IE8 bug.
						$window.bind('resize.' + prefix, publicMethod.position);
					}, 1);
				}

				if (loadedCallback) {
					loadedCallback();
				}
			},
			step: modalDimensions
		});
	};

	publicMethod.resize = function (options) {
		var scrolltop;
		
		if (open) {
			options = options || {};
			
			if (options.width) {
				settings.w = setSize(options.width, 'x') - loadedWidth - interfaceWidth;
			}

			if (options.innerWidth) {
				settings.w = setSize(options.innerWidth, 'x');
			}

			$loaded.css({width: settings.w});
			
			if (options.height) {
				settings.h = setSize(options.height, 'y') - loadedHeight - interfaceHeight;
			}

			if (options.innerHeight) {
				settings.h = setSize(options.innerHeight, 'y');
			}

			if (!options.innerHeight && !options.height) {
				scrolltop = $loaded.scrollTop();
				$loaded.css({height: "auto"});
				settings.h = $loaded.height();
			}

			$loaded.css({height: settings.h});

			if(scrolltop) {
				$loaded.scrollTop(scrolltop);
			}
			
			publicMethod.position(settings.get('transition') === "none" ? 0 : settings.get('speed'));
		}
	};

	publicMethod.prep = function (object) {
		if (!open) {
			return;
		}
		
		var callback, speed = settings.get('transition') === "none" ? 0 : settings.get('speed');

		$loaded.remove();

		$loaded = $tag(div, 'LoadedContent').append(object);
		
		function getWidth() {
			settings.w = settings.w || $loaded.width();
			settings.w = settings.mw && settings.mw < settings.w ? settings.mw : settings.w;
			return settings.w;
		}
		function getHeight() {
			settings.h = settings.h || $loaded.height();
			settings.h = settings.mh && settings.mh < settings.h ? settings.mh : settings.h;
			return settings.h;
		}
		
		$loaded.hide()
		.appendTo($loadingBay.show())// content has to be appended to the DOM for accurate size calculations.
		.css({width: getWidth(), overflow: settings.get('scrolling') ? 'auto' : 'hidden'})
		.css({height: getHeight()})// sets the height independently from the width in case the new width influences the value of height.
		.prependTo($content);
		
		$loadingBay.hide();
		
		// floating the IMG removes the bottom line-height and fixed a problem where IE miscalculates the width of the parent element as 100% of the document width.
		
		$(photo).css({'float': 'none'});

		setClass(settings.get('className'));

		callback = function () {
			var total = $related.length,
				iframe,
				complete;
			
			if (!open) {
				return;
			}
			
			function removeFilter() { // Needed for IE8 in versions of jQuery prior to 1.7.2
				if ($.support.opacity === false) {
					$box[0].style.removeAttribute('filter');
				}
			}
			
			complete = function () {
				clearTimeout(loadingTimer);
				$loadingOverlay.hide();
				trigger(event_complete);
				settings.get('onComplete');
			};

			
			$title.html(settings.get('title')).show();
			$loaded.show();
			
			if (total > 1) { // handle grouping
				if (typeof settings.get('current') === "string") {
					$current.html(settings.get('current').replace('{current}', index + 1).replace('{total}', total)).show();
				}
				
				$next[(settings.get('loop') || index < total - 1) ? "show" : "hide"]().html(settings.get('next'));
				$prev[(settings.get('loop') || index) ? "show" : "hide"]().html(settings.get('previous'));
				
				slideshow();
				
				// Preloads images within a rel group
				if (settings.get('preloading')) {
					$.each([getIndex(-1), getIndex(1)], function(){
						var img,
							i = $related[this],
							settings = new Settings(i, $.data(i, colorbox)),
							src = settings.get('href');

						if (src && isImage(settings, src)) {
							src = retinaUrl(settings, src);
							img = document.createElement('img');
							img.src = src;
						}
					});
				}
			} else {
				$groupControls.hide();
			}
			
			if (settings.get('iframe')) {
				iframe = document.createElement('iframe');
				
				if ('frameBorder' in iframe) {
					iframe.frameBorder = 0;
				}
				
				if ('allowTransparency' in iframe) {
					iframe.allowTransparency = "true";
				}

				if (!settings.get('scrolling')) {
					iframe.scrolling = "no";
				}
				
				$(iframe)
					.attr({
						src: settings.get('href'),
						name: (new Date()).getTime(), // give the iframe a unique name to prevent caching
						'class': prefix + 'Iframe',
						allowFullScreen : true // allow HTML5 video to go fullscreen
					})
					.one('load', complete)
					.appendTo($loaded);
				
				$events.one(event_purge, function () {
					iframe.src = "//about:blank";
				});

				if (settings.get('fastIframe')) {
					$(iframe).trigger('load');
				}
			} else {
				complete();
			}
			
			if (settings.get('transition') === 'fade') {
				$box.fadeTo(speed, 1, removeFilter);
			} else {
				removeFilter();
			}
		};
		
		if (settings.get('transition') === 'fade') {
			$box.fadeTo(speed, 0, function () {
				publicMethod.position(0, callback);
			});
		} else {
			publicMethod.position(speed, callback);
		}
	};

	function load () {
		var href, setResize, prep = publicMethod.prep, $inline, request = ++requests;
		
		active = true;
		
		photo = false;
		
		trigger(event_purge);
		trigger(event_load);
		settings.get('onLoad');
		
		settings.h = settings.get('height') ?
				setSize(settings.get('height'), 'y') - loadedHeight - interfaceHeight :
				settings.get('innerHeight') && setSize(settings.get('innerHeight'), 'y');
		
		settings.w = settings.get('width') ?
				setSize(settings.get('width'), 'x') - loadedWidth - interfaceWidth :
				settings.get('innerWidth') && setSize(settings.get('innerWidth'), 'x');
		
		// Sets the minimum dimensions for use in image scaling
		settings.mw = settings.w;
		settings.mh = settings.h;
		
		// Re-evaluate the minimum width and height based on maxWidth and maxHeight values.
		// If the width or height exceed the maxWidth or maxHeight, use the maximum values instead.
		if (settings.get('maxWidth')) {
			settings.mw = setSize(settings.get('maxWidth'), 'x') - loadedWidth - interfaceWidth;
			settings.mw = settings.w && settings.w < settings.mw ? settings.w : settings.mw;
		}
		if (settings.get('maxHeight')) {
			settings.mh = setSize(settings.get('maxHeight'), 'y') - loadedHeight - interfaceHeight;
			settings.mh = settings.h && settings.h < settings.mh ? settings.h : settings.mh;
		}
		
		href = settings.get('href');
		
		loadingTimer = setTimeout(function () {
			$loadingOverlay.show();
		}, 100);
		
		if (settings.get('inline')) {
			var $target = $(href);
			// Inserts an empty placeholder where inline content is being pulled from.
			// An event is bound to put inline content back when Colorbox closes or loads new content.
			$inline = $('<div>').hide().insertBefore($target);

			$events.one(event_purge, function () {
				$inline.replaceWith($target);
			});

			prep($target);
		} else if (settings.get('iframe')) {
			// IFrame element won't be added to the DOM until it is ready to be displayed,
			// to avoid problems with DOM-ready JS that might be trying to run in that iframe.
			prep(" ");
		} else if (settings.get('html')) {
			prep(settings.get('html'));
		} else if (isImage(settings, href)) {

			href = retinaUrl(settings, href);

			photo = new Image();

			$(photo)
			.addClass(prefix + 'Photo')
			.bind('error',function () {
				prep($tag(div, 'Error').html(settings.get('imgError')));
			})
			.one('load', function () {
				if (request !== requests) {
					return;
				}

				// A small pause because some browsers will occassionaly report a 
				// img.width and img.height of zero immediately after the img.onload fires
				setTimeout(function(){
					var percent;

					$.each(['alt', 'longdesc', 'aria-describedby'], function(i,val){
						var attr = $(settings.el).attr(val) || $(settings.el).attr('data-'+val);
						if (attr) {
							photo.setAttribute(val, attr);
						}
					});

					if (settings.get('retinaImage') && window.devicePixelRatio > 1) {
						photo.height = photo.height / window.devicePixelRatio;
						photo.width = photo.width / window.devicePixelRatio;
					}

					if (settings.get('scalePhotos')) {
						setResize = function () {
							photo.height -= photo.height * percent;
							photo.width -= photo.width * percent;
						};
						if (settings.mw && photo.width > settings.mw) {
							percent = (photo.width - settings.mw) / photo.width;
							setResize();
						}
						if (settings.mh && photo.height > settings.mh) {
							percent = (photo.height - settings.mh) / photo.height;
							setResize();
						}
					}
					
					if (settings.h) {
						photo.style.marginTop = Math.max(settings.mh - photo.height, 0) / 2 + 'px';
					}
					
					if ($related[1] && (settings.get('loop') || $related[index + 1])) {
						photo.style.cursor = 'pointer';
						photo.onclick = function () {
							publicMethod.next();
						};
					}

					photo.style.width = photo.width + 'px';
					photo.style.height = photo.height + 'px';
					prep(photo);
				}, 1);
			});
			
			photo.src = href;

		} else if (href) {
			$loadingBay.load(href, settings.get('data'), function (data, status) {
				if (request === requests) {
					prep(status === 'error' ? $tag(div, 'Error').html(settings.get('xhrError')) : $(this).contents());
				}
			});
		}
	}
		
	// Navigates to the next page/image in a set.
	publicMethod.next = function () {
		if (!active && $related[1] && (settings.get('loop') || $related[index + 1])) {
			index = getIndex(1);
			launch($related[index]);
		}
	};
	
	publicMethod.prev = function () {
		if (!active && $related[1] && (settings.get('loop') || index)) {
			index = getIndex(-1);
			launch($related[index]);
		}
	};

	// Note: to use this within an iframe use the following format: parent.jQuery.colorbox.close();
	publicMethod.close = function () {
		if (open && !closing) {
			
			closing = true;
			open = false;
			trigger(event_cleanup);
			settings.get('onCleanup');
			$window.unbind('.' + prefix);
			$overlay.fadeTo(settings.get('fadeOut') || 0, 0);
			
			$box.stop().fadeTo(settings.get('fadeOut') || 0, 0, function () {
				$box.hide();
				$overlay.hide();
				trigger(event_purge);
				$loaded.remove();
				
				setTimeout(function () {
					closing = false;
					trigger(event_closed);
					settings.get('onClosed');
				}, 1);
			});
		}
	};

	// Removes changes Colorbox made to the document, but does not remove the plugin.
	publicMethod.remove = function () {
		if (!$box) { return; }

		$box.stop();
		$.colorbox.close();
		$box.stop(false, true).remove();
		$overlay.remove();
		closing = false;
		$box = null;
		$('.' + boxElement)
			.removeData(colorbox)
			.removeClass(boxElement);

		$(document).unbind('click.'+prefix).unbind('keydown.'+prefix);
	};

	// A method for fetching the current element Colorbox is referencing.
	// returns a jQuery object.
	publicMethod.element = function () {
		return $(settings.el);
	};

	publicMethod.settings = defaults;

}(jQuery, document, window));

/** javascripts/vendor/jquery.jcarousel.min.js **/
/*! jCarousel - v0.3.0 - 2013-11-22
* http://sorgalla.com/jcarousel
* Copyright (c) 2013 Jan Sorgalla; Licensed MIT */
(function(t){"use strict";var i=t.jCarousel={};i.version="0.3.0";var s=/^([+\-]=)?(.+)$/;i.parseTarget=function(t){var i=!1,e="object"!=typeof t?s.exec(t):null;return e?(t=parseInt(e[2],10)||0,e[1]&&(i=!0,"-="===e[1]&&(t*=-1))):"object"!=typeof t&&(t=parseInt(t,10)||0),{target:t,relative:i}},i.detectCarousel=function(t){for(var i;t.length>0;){if(i=t.filter("[data-jcarousel]"),i.length>0)return i;if(i=t.find("[data-jcarousel]"),i.length>0)return i;t=t.parent()}return null},i.base=function(s){return{version:i.version,_options:{},_element:null,_carousel:null,_init:t.noop,_create:t.noop,_destroy:t.noop,_reload:t.noop,create:function(){return this._element.attr("data-"+s.toLowerCase(),!0).data(s,this),!1===this._trigger("create")?this:(this._create(),this._trigger("createend"),this)},destroy:function(){return!1===this._trigger("destroy")?this:(this._destroy(),this._trigger("destroyend"),this._element.removeData(s).removeAttr("data-"+s.toLowerCase()),this)},reload:function(t){return!1===this._trigger("reload")?this:(t&&this.options(t),this._reload(),this._trigger("reloadend"),this)},element:function(){return this._element},options:function(i,s){if(0===arguments.length)return t.extend({},this._options);if("string"==typeof i){if(s===void 0)return this._options[i]===void 0?null:this._options[i];this._options[i]=s}else this._options=t.extend({},this._options,i);return this},carousel:function(){return this._carousel||(this._carousel=i.detectCarousel(this.options("carousel")||this._element),this._carousel||t.error('Could not detect carousel for plugin "'+s+'"')),this._carousel},_trigger:function(i,e,r){var n,o=!1;return r=[this].concat(r||[]),(e||this._element).each(function(){n=t.Event((s+":"+i).toLowerCase()),t(this).trigger(n,r),n.isDefaultPrevented()&&(o=!0)}),!o}}},i.plugin=function(s,e){var r=t[s]=function(i,s){this._element=t(i),this.options(s),this._init(),this.create()};return r.fn=r.prototype=t.extend({},i.base(s),e),t.fn[s]=function(i){var e=Array.prototype.slice.call(arguments,1),n=this;return"string"==typeof i?this.each(function(){var r=t(this).data(s);if(!r)return t.error("Cannot call methods on "+s+" prior to initialization; "+'attempted to call method "'+i+'"');if(!t.isFunction(r[i])||"_"===i.charAt(0))return t.error('No such method "'+i+'" for '+s+" instance");var o=r[i].apply(r,e);return o!==r&&o!==void 0?(n=o,!1):void 0}):this.each(function(){var e=t(this).data(s);e instanceof r?e.reload(i):new r(this,i)}),n},r}})(jQuery),function(t,i){"use strict";var s=function(t){return parseFloat(t)||0};t.jCarousel.plugin("jcarousel",{animating:!1,tail:0,inTail:!1,resizeTimer:null,lt:null,vertical:!1,rtl:!1,circular:!1,underflow:!1,relative:!1,_options:{list:function(){return this.element().children().eq(0)},items:function(){return this.list().children()},animation:400,transitions:!1,wrap:null,vertical:null,rtl:null,center:!1},_list:null,_items:null,_target:null,_first:null,_last:null,_visible:null,_fullyvisible:null,_init:function(){var t=this;return this.onWindowResize=function(){t.resizeTimer&&clearTimeout(t.resizeTimer),t.resizeTimer=setTimeout(function(){t.reload()},100)},this},_create:function(){this._reload(),t(i).on("resize.jcarousel",this.onWindowResize)},_destroy:function(){t(i).off("resize.jcarousel",this.onWindowResize)},_reload:function(){this.vertical=this.options("vertical"),null==this.vertical&&(this.vertical=this.list().height()>this.list().width()),this.rtl=this.options("rtl"),null==this.rtl&&(this.rtl=function(i){if("rtl"===(""+i.attr("dir")).toLowerCase())return!0;var s=!1;return i.parents("[dir]").each(function(){return/rtl/i.test(t(this).attr("dir"))?(s=!0,!1):void 0}),s}(this._element)),this.lt=this.vertical?"top":"left",this.relative="relative"===this.list().css("position"),this._list=null,this._items=null;var i=this._target&&this.index(this._target)>=0?this._target:this.closest();this.circular="circular"===this.options("wrap"),this.underflow=!1;var s={left:0,top:0};return i.length>0&&(this._prepare(i),this.list().find("[data-jcarousel-clone]").remove(),this._items=null,this.underflow=this._fullyvisible.length>=this.items().length,this.circular=this.circular&&!this.underflow,s[this.lt]=this._position(i)+"px"),this.move(s),this},list:function(){if(null===this._list){var i=this.options("list");this._list=t.isFunction(i)?i.call(this):this._element.find(i)}return this._list},items:function(){if(null===this._items){var i=this.options("items");this._items=(t.isFunction(i)?i.call(this):this.list().find(i)).not("[data-jcarousel-clone]")}return this._items},index:function(t){return this.items().index(t)},closest:function(){var i,e=this,r=this.list().position()[this.lt],n=t(),o=!1,l=this.vertical?"bottom":this.rtl&&!this.relative?"left":"right";return this.rtl&&this.relative&&!this.vertical&&(r+=this.list().width()-this.clipping()),this.items().each(function(){if(n=t(this),o)return!1;var a=e.dimension(n);if(r+=a,r>=0){if(i=a-s(n.css("margin-"+l)),!(0>=Math.abs(r)-a+i/2))return!1;o=!0}}),n},target:function(){return this._target},first:function(){return this._first},last:function(){return this._last},visible:function(){return this._visible},fullyvisible:function(){return this._fullyvisible},hasNext:function(){if(!1===this._trigger("hasnext"))return!0;var t=this.options("wrap"),i=this.items().length-1;return i>=0&&(t&&"first"!==t||i>this.index(this._last)||this.tail&&!this.inTail)?!0:!1},hasPrev:function(){if(!1===this._trigger("hasprev"))return!0;var t=this.options("wrap");return this.items().length>0&&(t&&"last"!==t||this.index(this._first)>0||this.tail&&this.inTail)?!0:!1},clipping:function(){return this._element["inner"+(this.vertical?"Height":"Width")]()},dimension:function(t){return t["outer"+(this.vertical?"Height":"Width")](!0)},scroll:function(i,s,e){if(this.animating)return this;if(!1===this._trigger("scroll",null,[i,s]))return this;t.isFunction(s)&&(e=s,s=!0);var r=t.jCarousel.parseTarget(i);if(r.relative){var n,o,l,a,h,u,c,f,d=this.items().length-1,_=Math.abs(r.target),p=this.options("wrap");if(r.target>0){var v=this.index(this._last);if(v>=d&&this.tail)this.inTail?"both"===p||"last"===p?this._scroll(0,s,e):t.isFunction(e)&&e.call(this,!1):this._scrollTail(s,e);else if(n=this.index(this._target),this.underflow&&n===d&&("circular"===p||"both"===p||"last"===p)||!this.underflow&&v===d&&("both"===p||"last"===p))this._scroll(0,s,e);else if(l=n+_,this.circular&&l>d){for(f=d,h=this.items().get(-1);l>f++;)h=this.items().eq(0),u=this._visible.index(h)>=0,u&&h.after(h.clone(!0).attr("data-jcarousel-clone",!0)),this.list().append(h),u||(c={},c[this.lt]=this.dimension(h),this.moveBy(c)),this._items=null;this._scroll(h,s,e)}else this._scroll(Math.min(l,d),s,e)}else if(this.inTail)this._scroll(Math.max(this.index(this._first)-_+1,0),s,e);else if(o=this.index(this._first),n=this.index(this._target),a=this.underflow?n:o,l=a-_,0>=a&&(this.underflow&&"circular"===p||"both"===p||"first"===p))this._scroll(d,s,e);else if(this.circular&&0>l){for(f=l,h=this.items().get(0);0>f++;){h=this.items().eq(-1),u=this._visible.index(h)>=0,u&&h.after(h.clone(!0).attr("data-jcarousel-clone",!0)),this.list().prepend(h),this._items=null;var g=this.dimension(h);c={},c[this.lt]=-g,this.moveBy(c)}this._scroll(h,s,e)}else this._scroll(Math.max(l,0),s,e)}else this._scroll(r.target,s,e);return this._trigger("scrollend"),this},moveBy:function(t,i){var e=this.list().position(),r=1,n=0;return this.rtl&&!this.vertical&&(r=-1,this.relative&&(n=this.list().width()-this.clipping())),t.left&&(t.left=e.left+n+s(t.left)*r+"px"),t.top&&(t.top=e.top+n+s(t.top)*r+"px"),this.move(t,i)},move:function(i,s){s=s||{};var e=this.options("transitions"),r=!!e,n=!!e.transforms,o=!!e.transforms3d,l=s.duration||0,a=this.list();if(!r&&l>0)return a.animate(i,s),void 0;var h=s.complete||t.noop,u={};if(r){var c=a.css(["transitionDuration","transitionTimingFunction","transitionProperty"]),f=h;h=function(){t(this).css(c),f.call(this)},u={transitionDuration:(l>0?l/1e3:0)+"s",transitionTimingFunction:e.easing||s.easing,transitionProperty:l>0?function(){return n||o?"all":i.left?"left":"top"}():"none",transform:"none"}}o?u.transform="translate3d("+(i.left||0)+","+(i.top||0)+",0)":n?u.transform="translate("+(i.left||0)+","+(i.top||0)+")":t.extend(u,i),r&&l>0&&a.one("transitionend webkitTransitionEnd oTransitionEnd otransitionend MSTransitionEnd",h),a.css(u),0>=l&&a.each(function(){h.call(this)})},_scroll:function(i,s,e){if(this.animating)return t.isFunction(e)&&e.call(this,!1),this;if("object"!=typeof i?i=this.items().eq(i):i.jquery===void 0&&(i=t(i)),0===i.length)return t.isFunction(e)&&e.call(this,!1),this;this.inTail=!1,this._prepare(i);var r=this._position(i),n=this.list().position()[this.lt];if(r===n)return t.isFunction(e)&&e.call(this,!1),this;var o={};return o[this.lt]=r+"px",this._animate(o,s,e),this},_scrollTail:function(i,s){if(this.animating||!this.tail)return t.isFunction(s)&&s.call(this,!1),this;var e=this.list().position()[this.lt];this.rtl&&this.relative&&!this.vertical&&(e+=this.list().width()-this.clipping()),this.rtl&&!this.vertical?e+=this.tail:e-=this.tail,this.inTail=!0;var r={};return r[this.lt]=e+"px",this._update({target:this._target.next(),fullyvisible:this._fullyvisible.slice(1).add(this._visible.last())}),this._animate(r,i,s),this},_animate:function(i,s,e){if(e=e||t.noop,!1===this._trigger("animate"))return e.call(this,!1),this;this.animating=!0;var r=this.options("animation"),n=t.proxy(function(){this.animating=!1;var t=this.list().find("[data-jcarousel-clone]");t.length>0&&(t.remove(),this._reload()),this._trigger("animateend"),e.call(this,!0)},this),o="object"==typeof r?t.extend({},r):{duration:r},l=o.complete||t.noop;return s===!1?o.duration=0:t.fx.speeds[o.duration]!==void 0&&(o.duration=t.fx.speeds[o.duration]),o.complete=function(){n(),l.call(this)},this.move(i,o),this},_prepare:function(i){var e,r,n,o,l=this.index(i),a=l,h=this.dimension(i),u=this.clipping(),c=this.vertical?"bottom":this.rtl?"left":"right",f=this.options("center"),d={target:i,first:i,last:i,visible:i,fullyvisible:u>=h?i:t()};if(f&&(h/=2,u/=2),u>h)for(;;){if(e=this.items().eq(++a),0===e.length){if(!this.circular)break;if(e=this.items().eq(0),i.get(0)===e.get(0))break;if(r=this._visible.index(e)>=0,r&&e.after(e.clone(!0).attr("data-jcarousel-clone",!0)),this.list().append(e),!r){var _={};_[this.lt]=this.dimension(e),this.moveBy(_)}this._items=null}if(o=this.dimension(e),0===o)break;if(h+=o,d.last=e,d.visible=d.visible.add(e),n=s(e.css("margin-"+c)),u>=h-n&&(d.fullyvisible=d.fullyvisible.add(e)),h>=u)break}if(!this.circular&&!f&&u>h)for(a=l;;){if(0>--a)break;if(e=this.items().eq(a),0===e.length)break;if(o=this.dimension(e),0===o)break;if(h+=o,d.first=e,d.visible=d.visible.add(e),n=s(e.css("margin-"+c)),u>=h-n&&(d.fullyvisible=d.fullyvisible.add(e)),h>=u)break}return this._update(d),this.tail=0,f||"circular"===this.options("wrap")||"custom"===this.options("wrap")||this.index(d.last)!==this.items().length-1||(h-=s(d.last.css("margin-"+c)),h>u&&(this.tail=h-u)),this},_position:function(t){var i=this._first,s=i.position()[this.lt],e=this.options("center"),r=e?this.clipping()/2-this.dimension(i)/2:0;return this.rtl&&!this.vertical?(s-=this.relative?this.list().width()-this.dimension(i):this.clipping()-this.dimension(i),s+=r):s-=r,!e&&(this.index(t)>this.index(i)||this.inTail)&&this.tail?(s=this.rtl&&!this.vertical?s-this.tail:s+this.tail,this.inTail=!0):this.inTail=!1,-s},_update:function(i){var s,e=this,r={target:this._target||t(),first:this._first||t(),last:this._last||t(),visible:this._visible||t(),fullyvisible:this._fullyvisible||t()},n=this.index(i.first||r.first)<this.index(r.first),o=function(s){var o=[],l=[];i[s].each(function(){0>r[s].index(this)&&o.push(this)}),r[s].each(function(){0>i[s].index(this)&&l.push(this)}),n?o=o.reverse():l=l.reverse(),e._trigger(s+"in",t(o)),e._trigger(s+"out",t(l)),e["_"+s]=i[s]};for(s in i)o(s);return this}})}(jQuery,window),function(t){"use strict";t.jcarousel.fn.scrollIntoView=function(i,s,e){var r,n=t.jCarousel.parseTarget(i),o=this.index(this._fullyvisible.first()),l=this.index(this._fullyvisible.last());if(r=n.relative?0>n.target?Math.max(0,o+n.target):l+n.target:"object"!=typeof n.target?n.target:this.index(n.target),o>r)return this.scroll(r,s,e);if(r>=o&&l>=r)return t.isFunction(e)&&e.call(this,!1),this;for(var a,h=this.items(),u=this.clipping(),c=this.vertical?"bottom":this.rtl?"left":"right",f=0;;){if(a=h.eq(r),0===a.length)break;if(f+=this.dimension(a),f>=u){var d=parseFloat(a.css("margin-"+c))||0;f-d!==u&&r++;break}if(0>=r)break;r--}return this.scroll(r,s,e)}}(jQuery),function(t){"use strict";t.jCarousel.plugin("jcarouselControl",{_options:{target:"+=1",event:"click",method:"scroll"},_active:null,_init:function(){this.onDestroy=t.proxy(function(){this._destroy(),this.carousel().one("jcarousel:createend",t.proxy(this._create,this))},this),this.onReload=t.proxy(this._reload,this),this.onEvent=t.proxy(function(i){i.preventDefault();var s=this.options("method");t.isFunction(s)?s.call(this):this.carousel().jcarousel(this.options("method"),this.options("target"))},this)},_create:function(){this.carousel().one("jcarousel:destroy",this.onDestroy).on("jcarousel:reloadend jcarousel:scrollend",this.onReload),this._element.on(this.options("event")+".jcarouselcontrol",this.onEvent),this._reload()},_destroy:function(){this._element.off(".jcarouselcontrol",this.onEvent),this.carousel().off("jcarousel:destroy",this.onDestroy).off("jcarousel:reloadend jcarousel:scrollend",this.onReload)},_reload:function(){var i,s=t.jCarousel.parseTarget(this.options("target")),e=this.carousel();if(s.relative)i=e.jcarousel(s.target>0?"hasNext":"hasPrev");else{var r="object"!=typeof s.target?e.jcarousel("items").eq(s.target):s.target;i=e.jcarousel("target").index(r)>=0}return this._active!==i&&(this._trigger(i?"active":"inactive"),this._active=i),this}})}(jQuery),function(t){"use strict";t.jCarousel.plugin("jcarouselPagination",{_options:{perPage:null,item:function(t){return'<a href="#'+t+'">'+t+"</a>"},event:"click",method:"scroll"},_pages:{},_items:{},_currentPage:null,_init:function(){this.onDestroy=t.proxy(function(){this._destroy(),this.carousel().one("jcarousel:createend",t.proxy(this._create,this))},this),this.onReload=t.proxy(this._reload,this),this.onScroll=t.proxy(this._update,this)},_create:function(){this.carousel().one("jcarousel:destroy",this.onDestroy).on("jcarousel:reloadend",this.onReload).on("jcarousel:scrollend",this.onScroll),this._reload()},_destroy:function(){this._clear(),this.carousel().off("jcarousel:destroy",this.onDestroy).off("jcarousel:reloadend",this.onReload).off("jcarousel:scrollend",this.onScroll)},_reload:function(){var i=this.options("perPage");if(this._pages={},this._items={},t.isFunction(i)&&(i=i.call(this)),null==i)this._pages=this._calculatePages();else for(var s,e=parseInt(i,10)||0,r=this.carousel().jcarousel("items"),n=1,o=0;;){if(s=r.eq(o++),0===s.length)break;this._pages[n]=this._pages[n]?this._pages[n].add(s):s,0===o%e&&n++}this._clear();var l=this,a=this.carousel().data("jcarousel"),h=this._element,u=this.options("item");t.each(this._pages,function(i,s){var e=l._items[i]=t(u.call(l,i,s));e.on(l.options("event")+".jcarouselpagination",t.proxy(function(){var t=s.eq(0);if(a.circular){var e=a.index(a.target()),r=a.index(t);parseFloat(i)>parseFloat(l._currentPage)?e>r&&(t="+="+(a.items().length-e+r)):r>e&&(t="-="+(e+(a.items().length-r)))}a[this.options("method")](t)},l)),h.append(e)}),this._update()},_update:function(){var i,s=this.carousel().jcarousel("target");t.each(this._pages,function(t,e){return e.each(function(){return s.is(this)?(i=t,!1):void 0}),i?!1:void 0}),this._currentPage!==i&&(this._trigger("inactive",this._items[this._currentPage]),this._trigger("active",this._items[i])),this._currentPage=i},items:function(){return this._items},_clear:function(){this._element.empty(),this._currentPage=null},_calculatePages:function(){for(var t,i=this.carousel().data("jcarousel"),s=i.items(),e=i.clipping(),r=0,n=0,o=1,l={};;){if(t=s.eq(n++),0===t.length)break;l[o]=l[o]?l[o].add(t):t,r+=i.dimension(t),r>=e&&(o++,r=0)}return l}})}(jQuery),function(t){"use strict";t.jCarousel.plugin("jcarouselAutoscroll",{_options:{target:"+=1",interval:3e3,autostart:!0},_timer:null,_init:function(){this.onDestroy=t.proxy(function(){this._destroy(),this.carousel().one("jcarousel:createend",t.proxy(this._create,this))},this),this.onAnimateEnd=t.proxy(this.start,this)},_create:function(){this.carousel().one("jcarousel:destroy",this.onDestroy),this.options("autostart")&&this.start()},_destroy:function(){this.stop(),this.carousel().off("jcarousel:destroy",this.onDestroy)},start:function(){return this.stop(),this.carousel().one("jcarousel:animateend",this.onAnimateEnd),this._timer=setTimeout(t.proxy(function(){this.carousel().jcarousel("scroll",this.options("target"))},this),this.options("interval")),this},stop:function(){return this._timer&&(this._timer=clearTimeout(this._timer)),this.carousel().off("jcarousel:animateend",this.onAnimateEnd),this}})}(jQuery);
/** javascripts/vendor/jquery.lazyload.js **/
/*
 * Lazy Load - jQuery plugin for lazy loading images
 *
 * Copyright (c) 2007-2013 Mika Tuupola
 *
 * Licensed under the MIT license:
 *   http://www.opensource.org/licenses/mit-license.php
 *
 * Project home:
 *   http://www.appelsiini.net/projects/lazyload
 *
 * Version:  1.9.1
 *
 */

(function($, window, document, undefined) {
    var $window = $(window);

    $.fn.lazyload = function(options) {
        var elements = this;
        var $container;
        var settings = {
            threshold       : 0,
            failure_limit   : 0,
            event           : "scroll",
            effect          : "show",
            container       : window,
            data_attribute  : "original",
            skip_invisible  : true,
            appear          : null,
            load            : null,
            placeholder     : "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAAJcEhZcwAADsQAAA7EAZUrDhsAAAANSURBVBhXYzh8+PB/AAffA0nNPuCLAAAAAElFTkSuQmCC"
        };

        function update() {
            var counter = 0;

            elements.each(function() {
                var $this = $(this);
                if (settings.skip_invisible && !$this.is(":visible")) {
                    return;
                }
                if ($.abovethetop(this, settings) ||
                    $.leftofbegin(this, settings)) {
                        /* Nothing. */
                } else if (!$.belowthefold(this, settings) &&
                    !$.rightoffold(this, settings)) {
                        $this.trigger("appear");
                        /* if we found an image we'll load, reset the counter */
                        counter = 0;
                } else {
                    if (++counter > settings.failure_limit) {
                        return false;
                    }
                }
            });

        }

        if(options) {
            /* Maintain BC for a couple of versions. */
            if (undefined !== options.failurelimit) {
                options.failure_limit = options.failurelimit;
                delete options.failurelimit;
            }
            if (undefined !== options.effectspeed) {
                options.effect_speed = options.effectspeed;
                delete options.effectspeed;
            }

            $.extend(settings, options);
        }

        /* Cache container as jQuery as object. */
        $container = (settings.container === undefined ||
                      settings.container === window) ? $window : $(settings.container);

        /* Fire one scroll event per scroll. Not one scroll event per image. */
        if (0 === settings.event.indexOf("scroll")) {
            $container.bind(settings.event, function() {
                return update();
            });
        }

        this.each(function() {
            var self = this;
            var $self = $(self);

            self.loaded = false;

            /* If no src attribute given use data:uri. */
            if ($self.attr("src") === undefined || $self.attr("src") === false) {
                if ($self.is("img")) {
                    $self.attr("src", settings.placeholder);
                }
            }

            /* When appear is triggered load original image. */
            $self.one("appear", function() {
                if (!this.loaded) {
                    if (settings.appear) {
                        var elements_left = elements.length;
                        settings.appear.call(self, elements_left, settings);
                    }
                    $("<img />")
                        .bind("load", function() {

                            var original = $self.attr("data-" + settings.data_attribute);
                            $self.hide();
                            if ($self.is("img")) {
                                $self.attr("src", original);
                            } else {
                                $self.css("background-image", "url('" + original + "')");
                            }
                            $self[settings.effect](settings.effect_speed);

                            self.loaded = true;

                            /* Remove image from array so it is not looped next time. */
                            var temp = $.grep(elements, function(element) {
                                return !element.loaded;
                            });
                            elements = $(temp);

                            if (settings.load) {
                                var elements_left = elements.length;
                                settings.load.call(self, elements_left, settings);
                            }
                        })
                        .attr("src", $self.attr("data-" + settings.data_attribute));
                }
            });

            /* When wanted event is triggered load original image */
            /* by triggering appear.                              */
            if (0 !== settings.event.indexOf("scroll")) {
                $self.bind(settings.event, function() {
                    if (!self.loaded) {
                        $self.trigger("appear");
                    }
                });
            }
        });

        /* Check if something appears when window is resized. */
        $window.bind("resize", function() {
            update();
        });

        /* With IOS5 force loading images when navigating with back button. */
        /* Non optimal workaround. */
        if ((/(?:iphone|ipod|ipad).*os 5/gi).test(navigator.appVersion)) {
            $window.bind("pageshow", function(event) {
                if (event.originalEvent && event.originalEvent.persisted) {
                    elements.each(function() {
                        $(this).trigger("appear");
                    });
                }
            });
        }

        /* Force initial check if images should appear. */
        $(document).ready(function() {
            update();
        });

        return this;
    };

    /* Convenience methods in jQuery namespace.           */
    /* Use as  $.belowthefold(element, {threshold : 100, container : window}) */

    $.belowthefold = function(element, settings) {
        var fold;

        if (settings.container === undefined || settings.container === window) {
            fold = (window.innerHeight ? window.innerHeight : $window.height()) + $window.scrollTop();
        } else {
            fold = $(settings.container).offset().top + $(settings.container).height();
        }

        return fold <= $(element).offset().top - settings.threshold;
    };

    $.rightoffold = function(element, settings) {
        var fold;

        if (settings.container === undefined || settings.container === window) {
            fold = $window.width() + $window.scrollLeft();
        } else {
            fold = $(settings.container).offset().left + $(settings.container).width();
        }

        return fold <= $(element).offset().left - settings.threshold;
    };

    $.abovethetop = function(element, settings) {
        var fold;

        if (settings.container === undefined || settings.container === window) {
            fold = $window.scrollTop();
        } else {
            fold = $(settings.container).offset().top;
        }

        return fold >= $(element).offset().top + settings.threshold  + $(element).height();
    };

    $.leftofbegin = function(element, settings) {
        var fold;

        if (settings.container === undefined || settings.container === window) {
            fold = $window.scrollLeft();
        } else {
            fold = $(settings.container).offset().left;
        }

        return fold >= $(element).offset().left + settings.threshold + $(element).width();
    };

    $.inviewport = function(element, settings) {
         return !$.rightoffold(element, settings) && !$.leftofbegin(element, settings) &&
                !$.belowthefold(element, settings) && !$.abovethetop(element, settings);
     };

    /* Custom selectors for your convenience.   */
    /* Use as $("img:below-the-fold").something() or */
    /* $("img").filter(":below-the-fold").something() which is faster */

    $.extend($.expr[":"], {
        "below-the-fold" : function(a) { return $.belowthefold(a, {threshold : 0}); },
        "above-the-top"  : function(a) { return !$.belowthefold(a, {threshold : 0}); },
        "right-of-screen": function(a) { return $.rightoffold(a, {threshold : 0}); },
        "left-of-screen" : function(a) { return !$.rightoffold(a, {threshold : 0}); },
        "in-viewport"    : function(a) { return $.inviewport(a, {threshold : 0}); },
        /* Maintain BC for couple of versions. */
        "above-the-fold" : function(a) { return !$.belowthefold(a, {threshold : 0}); },
        "right-of-fold"  : function(a) { return $.rightoffold(a, {threshold : 0}); },
        "left-of-fold"   : function(a) { return !$.rightoffold(a, {threshold : 0}); }
    });

})(jQuery, window, document);

/** javascripts/vendor/jquery.cookie.js **/
/*!
 * jQuery Cookie Plugin v1.4.1
 * https://github.com/carhartl/jquery-cookie
 *
 * Copyright 2013 Klaus Hartl
 * Released under the MIT license
 */
(function (factory) {
	if (typeof define === 'function' && define.amd) {
		// AMD
		define(['jquery'], factory);
	} else if (typeof exports === 'object') {
		// CommonJS
		factory(require('jquery'));
	} else {
		// Browser globals
		factory(jQuery);
	}
}(function ($) {

	var pluses = /\+/g;

	function encode(s) {
		return config.raw ? s : encodeURIComponent(s);
	}

	function decode(s) {
		return config.raw ? s : decodeURIComponent(s);
	}

	function stringifyCookieValue(value) {
		return encode(config.json ? JSON.stringify(value) : String(value));
	}

	function parseCookieValue(s) {
		if (s.indexOf('"') === 0) {
			// This is a quoted cookie as according to RFC2068, unescape...
			s = s.slice(1, -1).replace(/\\"/g, '"').replace(/\\\\/g, '\\');
		}

		try {
			// Replace server-side written pluses with spaces.
			// If we can't decode the cookie, ignore it, it's unusable.
			// If we can't parse the cookie, ignore it, it's unusable.
			s = decodeURIComponent(s.replace(pluses, ' '));
			return config.json ? JSON.parse(s) : s;
		} catch(e) {}
	}

	function read(s, converter) {
		var value = config.raw ? s : parseCookieValue(s);
		return $.isFunction(converter) ? converter(value) : value;
	}

	var config = $.cookie = function (key, value, options) {

		// Write

		if (value !== undefined && !$.isFunction(value)) {
			options = $.extend({}, config.defaults, options);

			if (typeof options.expires === 'number') {
				var days = options.expires, t = options.expires = new Date();
				t.setTime(+t + days * 864e+5);
			}

			return (document.cookie = [
				encode(key), '=', stringifyCookieValue(value),
				options.expires ? '; expires=' + options.expires.toUTCString() : '', // use expires attribute, max-age is not supported by IE
				options.path    ? '; path=' + options.path : '',
				options.domain  ? '; domain=' + options.domain : '',
				options.secure  ? '; secure' : ''
			].join(''));
		}

		// Read

		var result = key ? undefined : {};

		// To prevent the for loop in the first place assign an empty array
		// in case there are no cookies at all. Also prevents odd result when
		// calling $.cookie().
		var cookies = document.cookie ? document.cookie.split('; ') : [];

		for (var i = 0, l = cookies.length; i < l; i++) {
			var parts = cookies[i].split('=');
			var name = decode(parts.shift());
			var cookie = parts.join('=');

			if (key && key === name) {
				// If second argument (value) is a function it's a converter...
				result = read(cookie, value);
				break;
			}

			// Prevent storing a cookie that we couldn't decode.
			if (!key && (cookie = read(cookie)) !== undefined) {
				result[name] = cookie;
			}
		}

		return result;
	};

	config.defaults = {};

	$.removeCookie = function (key, options) {
		if ($.cookie(key) === undefined) {
			return false;
		}

		// Must not alter options, thus extending a fresh object...
		$.cookie(key, '', $.extend({}, options, { expires: -1 }));
		return !$.cookie(key);
	};

}));

/** javascripts/jquery-visibility-extensions.js **/
jQuery.fn.visible = function() {
    'use strict';

    return this.css('visibility', 'visible');
};

jQuery.fn.invisible = function() {
    'use strict';

    return this.css('visibility', 'hidden');
};

jQuery.fn.visibilityToggle = function() {
    'use strict';

    return this.css('visibility', function(i, visibility) {
        return (visibility === 'visible') ? 'hidden' : 'visible';
    });
};
/** javascripts/readerOverlay.js **/
$(document).ready(function() {
    'use strict';

    var root = $('#reader-overlay');
    var readerArea = $('#reader');
    var images = root.find('img');

    var isLayerOpen = false;
    var fallbackImage = null;

    function clearImages() {
        images.each(function() {
            var image = $(this);
            if (fallbackImage === null) { fallbackImage = image.attr('src'); } // fallback image is already set (because we need CDN logic to get the URL)
            image.attr({'src': null});
        });
    }

    clearImages();
    images.each(function() {
        $(this).error(function() {
            var image = $(this);
            if (image.attr('src') !== null && typeof image.attr('src') !== "undefined") {image.attr({'src': fallbackImage});}  // set back to fallback if the image doesn't load
        });
    });

    function openReaderOverlayForDoi(doi){
        images.each(function(page) {
            var previewLink = "//page-one.live.cf.public.springer.com/chapter/" + doi + "/lookinside/00" + page + ".png";
            var image = $(this);
            image.attr({ 'src': previewLink });
        }).end().toggle();
        $('html').css({
            'overflow-y' : 'hidden'
        });
        isLayerOpen = true;
    }

    function makeDocumentScrollable(){
        var isLessThanIe9 = $("html").hasClass("lt-ie9");
        if(isLessThanIe9){
            $('html').removeAttr('style');
        } else {
            $('html').css({ 'overflow-y' : '' });
        }
    }

    function closeReaderOverlay(){
        root.hide();
        clearImages();
        makeDocumentScrollable();
        isLayerOpen = false;
    }

    // attach listeners so that reader overlay can be closed with escape or click next to the overlay
    $(document).on('click', function(e) {
        if(isLayerOpen && $(e.target).closest(readerArea).length === 0){ // click next to overlay?
            closeReaderOverlay();
        }
    });
    $(document).on('keydown', function(e) {
        if(isLayerOpen && e.keyCode === 27){ // ESC?
            closeReaderOverlay();
        }
    });

    window.readerOverlay = {
        openForDoi : openReaderOverlayForDoi,
        close : closeReaderOverlay
    };
});
/** javascripts/main.js **/
$(document).ready(function() {
    'use strict';

    function closeExpander(element) {
        element.next().slideUp('normal', function() {
            $(this).prev().removeClass('open');
        });
    }

    function openExpander(element) {
        element.next().slideDown('normal', function() {
            $(this).prev().addClass('open');
        });
    }

    /* EXPANDER
    ------------------------------------------------------------------------------*/
    $('dl.expander dt').each(function() {
        $(this).click(function() {
            $(this).siblings().each(function() {
                if ($(this).hasClass('open')) {
                   closeExpander($(this));
                }
            });
            if ($(this).hasClass('open')) {
                closeExpander($(this));
            } else {
                openExpander($(this));
            }
        });
    });

    /* BUTTON GROUP
    ------------------------------------------------------------------------------*/
    $('.btn-group .btn').click(function() {
        $(this).addClass('act').blur().siblings().removeClass('act');
    });

    /* TOGGLE USPS
    ------------------------------------------------------------------------------*/
    $('.product-page .usps .action-show-all').on('click', function(e) {
        e.preventDefault();
        $(this).closest('.usps').toggleClass('open');
    });
});

/** javascripts/cover.js **/
$(document).ready(function() {
    'use strict';

    var $root = $('.product-cover');

    $root.find('a.link-image').click(function() {
        var previewDoi = $(this).data('preview-doi');
        window.readerOverlay.openForDoi(previewDoi);
        var dataLayer = window.dataLayer = window.dataLayer || [];
        dataLayer.push({"event": "preview", "category": "Books", "label": "book"});
        return false;
    });
    var $root2 = $('.bibliographic-information');
    $root2.find('a.link-image-mobile').click(function() {
        var previewDoi = $(this).data('preview-doi');
        window.readerOverlay.openForDoi(previewDoi);
        var dataLayer = window.dataLayer = window.dataLayer || [];
        dataLayer.push({"event": "preview", "category": "Books", "label": "book"});
        return false;
    });
});

/** javascripts/productTabs.js **/
$(document).ready(function(){
    'use strict';

    var $root = $('.product-page');

    // copy dt of dl-list to ul-li tab panel
    $('dl.cms-tab-panel', $root).each(function(i) {
        $(this).prepend('<ul class="product-tabs cms-tab-pane" />');
        $('> dd', this).each(function(j) {
            $(this).addClass('cms-tab-content');
            $(this).attr('id', 'tab-content-' + i + '-' + j);
            if (j > 0) {
                $(this).hide();
            }
        });
        $('> dt', this).each(function(k) {
            $(this).parent('dl').find('ul.cms-tab-pane').append('<li class="tab-li" data-rel="tab-content-' + i + '-' + k + '">' + $(this).html() + '</li>');
        });

        // initialize the tab panel
        $('ul > li:first-child', this).addClass('active');
        $('> dt', this).first().addClass('active');
        $('> dd', this).first().show();
    });

    // prevent click defaults
    $('.cms-tab-pane > li a, .cms-tab-panel > dt a').on('click', function(e) {
        $(this).blur();
    });

    var expandContent = function($elem) {
        $elem.addClass('show-all');
    };

    function updateOnClick(id, useSlideup) {
        var expand = false;

        // tablet/widescreen
        var $clickedLi = $('li[data-rel=' + id + ']');
        if (!$clickedLi.hasClass('active')) {
            $clickedLi.siblings('li').removeClass('active');
            expand = true;
        }
        // smartphone
        var $clickedDt = $('#' + id).prev();
        if (!$clickedDt.hasClass('active')) {
            $clickedDt.siblings('dt').removeClass('active');
            expand = true;
        }

        var $dd = $('#' + id);
        if (expand) {
            if (useSlideup) {
                $dd.siblings('dd').slideUp();
                $dd.slideDown();
            } else {
                $dd.siblings('dd').hide();
                $dd.fadeIn();
            }
            expandContent($dd);
            $clickedLi.addClass('active');
            $clickedDt.addClass('active');
        } else {
            if (useSlideup) {
                $dd.slideUp();
            } else {
                $dd.hide();
            }
            $clickedLi.removeClass('active');
            $clickedDt.removeClass('active');
        }
    }

    // tab panel behaviour click
    $('.cms-tab-pane > li').on('click', function() {
        if (!$('#' + $(this).attr('data-rel')).is(':visible')) {
            updateOnClick($(this).attr('data-rel'), false);
        }
    });

    // tab panel behaviour on small screens
    $('.cms-tab-panel > dt').on('click', function() {
        updateOnClick($(this).next().attr('id'), true);
    });

    // dispatch a resize event should the video tab be clicked so the the player is sized appropriately
    $('.cms-tab-pane > li, .cms-tab-panel > dt').one('click', function() {
        if ($(this).find('a').attr('href') === '#video') {
            try {
                if (document.createEvent) {
                    // modern browsers
                    var ev = document.createEvent('Event');
                    ev.initEvent('resize', true, true);
                    window.dispatchEvent(ev);
                } else {
                    // IE
                    document.fireEvent('onresize');
                }
            } catch(ignore) {
                // fail silently
            }
        }
    });

    // SHOW ALL BUTTON
    $root.find('.cms-tab-panel dd .action-show-all')
        .each(function() {
            // check if show more button is needed; if no then expand automatically
            var content = $(this).closest('dd').find('> div > div')[0];
            if (content.scrollHeight <= content.clientHeight) {
                expandContent($(this).closest('dd'));
                $(this).closest('.action-footer').hide();
            }
        }).click(function(e) {
            // attach click action
            e.preventDefault();
            expandContent($(this).closest('dd'));
        });

    // SPECIAL USECASES:
    // about: open as given in hash
    var $about = $('.product-about');
    $about.find(".product-tabs a[href='" + window.location.hash + "']").click();

    // bibliographic on mobile: hide initially
    var $bibliographic = $('.product-bibliographic');
    var $bibTitle = $bibliographic.find('.cms-tab-panel > dt a');
    if ($bibTitle.is(':visible')) {
        $bibTitle.click();
    }
});

/** javascripts/buyThisBook.js **/
$(document).ready(function() {
    'use strict';

    var $root = $('.product-buy');

    $('.product-buy .buy').each(function(i, ele) {
        $(ele).click(function() {
            var requestData = {};
            requestData.isbn = $(ele).parent().children('input[name=isbn]').val();
            requestData.price = $(ele).parent().children('input[name=price]').val();
            requestData.gratisReason = $(ele).parent().children('input[name=gratisReason]').val();

            var title = $(ele).parent().children('input[name=title]').val();
            jsRoutes.controllers.edition.BasketController.addRendition().ajax({
                contentType: 'application/json; charset=UTF-8',
                data: JSON.stringify(requestData),
                success: function(response) {
                    $.colorbox({
                        html: response,
                        maxWidth: '557px',
                        width: '86%'
                    });
                    wt.sendinfo({
                        contentId: wt.contentId,
                        product: requestData.isbn.replace(/-/g,'') + '_' + title,
                        productStatus: 'add'
                    });
                    window.basketWidget.refresh();
                },
                error: function() {
                    // console.log('error');
                }
            });
            return false;
        });
    });

    $('.download-section .actions .btn').on('click', function(event) {
        event.preventDefault();
        var $btn = $(this);
        if ($btn.hasClass('loading')) {
            return false;
        }

        var href = $btn.attr('href');

        $btn.addClass('loading');
        $btn.attr('disabled', 'disabled');

        $.get(href, function(data) {
            window.location.href = data;
        }).always(function() {
            $btn.removeClass('loading');
            $btn.removeAttr('disabled');
        });

        return false;
    });

    /**
     * Opens a rendition flap based on a hash provided.
     *
     * @param {String} hash The anchor parameter of a URL without the # sign
     */
    var openRenditionFlapByHash = function(hash) {
        var hashRenditionRegex = new RegExp('(otherversion=)(rental|[0-9-]*)', 'i');
        if (hash !== null && hash !== '') {
            var hashRenditionMatcher = hashRenditionRegex.exec(hash);
            // only proceed if the anchor matches the regex and has appropriate length
            if (hashRenditionMatcher !== null && hashRenditionMatcher.length >= 3) {
                var renditionFlapClass = 'dt.buy-rendition-' + hashRenditionMatcher[2];
                if ($root.find(renditionFlapClass).size() > 0) {
                    // only handle visibility if there is an element with the rendition flap class
                    $root.find('dt').not(renditionFlapClass).removeClass('open');
                    // don't use .click() to avoid animations
                    $root.find(renditionFlapClass).addClass('open');
                }
            }
        }
    };

    openRenditionFlapByHash(window.location.hash.substring(1));
});

/** javascripts/socialMediaIcons.js **/
jQuery(document).ready(function($) {
    'use strict';

    var TWITTER_MSG_MAX_LENGTH = 130;  // 140 - " #springer".length
    var url = encodeURIComponent(location.href);
    var fullTitle = encodeURIComponent(String(document.title.split(/\|/)[0]));
    var desc = encodeURIComponent($('meta[name=description]').attr('content'));

    function openWindow(url, width, height) {
        var leftPosition, topPosition;
        leftPosition = (window.screen.width / 2) - ((width / 2) + 10);
        topPosition = (window.screen.height / 2) - ((height / 2) + 50);
        window.open(url, '', 'status=no,height=' + height + ',width=' + width + ',resizable=yes,left=' + leftPosition + ',top=' + topPosition + ',screenX=' + leftPosition + ',screenY=' + topPosition + ',toolbar=no,menubar=no,scrollbars=no,location=no,directories=no');
    }

    var root = $('.product-share .cms-social-buttons');
    root.find('#tool-twitter').click(function() {
        var charsAllowed = TWITTER_MSG_MAX_LENGTH - location.href.length;
        var title = String(document.title.split(/\|/)[0]);
        if (document.title.length > charsAllowed) {
            title = title.substring(0, charsAllowed);
            title = encodeURIComponent(title.substring(0, title.lastIndexOf(' ')));
        }else {
            title = encodeURIComponent(title);
        }
        var target = 'https://twitter.com/share?hashtags=springer' + '&url=' + url + '&text=' + title;
        openWindow(target, 545, 433);
    });

    root.find('#tool-facebook').click(function() {
        var target = 'http://www.facebook.com/sharer/sharer.php?s=100&p[url]=' + url + '&p[title]=' + fullTitle +
            '&p[summary]=' + desc;
        openWindow(target, 545, 433);
    });

    root.find('#tool-linkedin').click(function() {
        var target = 'http://www.linkedin.com/shareArticle?mini=true&url=' + url + '&title=' + fullTitle + '&summary=' +
            desc;
        openWindow(target, 545, 433);
    });

    root.find('#tool-weibo').click(function() {
        var target = 'http://service.weibo.com/share/share.php?url=' + url + '&appkey=' + this['data-key'] + '&title=' +
            fullTitle + '&summary=' + desc + '&pic=&ralateUid=&language=zh_cn';
        openWindow(target, 545, 433);
    });

    root.find('#tool-gplus').click(function() {
        var target = 'https://plus.google.com/share?url=' + url + '&title=' + fullTitle;
        openWindow(target, 545, 433);
    });
});


/** javascripts/tocController.js **/
$(document).ready(function() {
    'use strict';

    var root = $('.product-toc');

    var updateRemainingCounter = function() {
        var remainingInvisible = root.find('.toc-hidden:first li').length;
        root.find('.action-show-all span').text(remainingInvisible);
        if (remainingInvisible === 0) {
            root.find('.action-show-all').invisible();
        } else {
            root.find('.action-show-all').visible();
        }
    };

    updateRemainingCounter();
    root.find('.action-show-all').click(function(e) {
        e.preventDefault();
        root.find('.toc-hidden:first').toggleClass('toc-hidden').slideDown({
            complete: function() {
                updateRemainingCounter();
            }
        });
    });
    root.find('dl.expander > dt').click(function() {
        if ($(this).hasClass('open')) {
            $(this).closest('.expander').find('.toc:not(:first)').addClass('toc-hidden').hide();
            updateRemainingCounter();
        }
    });

    root.find('.toc-item.chapter-item').each(function() {
        $(this).click(function() {
            if ($(this).hasClass('open')) {
                $(this).find('.detail').slideUp('normal', function() {
                    $(this).parent().removeClass('open');
                });
            } else {
                $(this).find('.detail').slideDown('normal', function() {
                    $(this).parent().addClass('open');
                    wt.sendinfo({linkId:'expand.chapter'});
                });
            }
        });
    });

    root.find('.buy').each(function(i, ele) {
        $(ele).click(function() {
            var requestData = {};
            requestData.chapterId = $(ele).parent().find('input[name=chapterId]').val();
            requestData.isbn = $(ele).parent().find('input[name=isbn]').val();
            requestData.price = $(ele).parent().find('input[name=price]').val();
            var doi = $(ele).parent().find('input[name=doi]').val();
            var title = $(ele).parent().find('input[name=title]').val();

            jsRoutes.controllers.edition.BasketController.addChapter().ajax({
                contentType: 'application/json; charset=UTF-8',
                data: JSON.stringify(requestData),
                success: function(response) {
                    $.colorbox({
                        html: response,
                        maxWidth: '557px',
                        width: '86%',
                        onComplete: function() {
                          if (window.MathJax) {
                            window.MathJax.Hub.Typeset($.colorbox);
                          }
                        }
                    });
                    wt.sendinfo({
                        linkId: 'chapter_add_to_cart',
                        contentId: wt.contentId,
                        product: doi + '_' + title,
                        productStatus: 'add',
                        productCategory : {
                            1 : 'ppv'
                        }
                    });
                },
                error: function() {
                    //alert('add to basket failed');
                }
            });
            return false;
        });
    });
    root.find('.toc-item .preview').click(function(){
        var doi = $(this).siblings('input[name=doi]').get(0).value;
        window.readerOverlay.openForDoi(doi);
        var dataLayer = window.dataLayer = window.dataLayer || [];
        dataLayer.push({"event": "preview", "category": "Books", "label": "chapter"});
        return false;
    });
});



/** javascripts/recommendationController.js **/
$(document).ready(function() {
    'use strict';

    var root = $('.product-recommendation');

    /* PAGING CAROUSEL */
    var configurePagingCarousel = function() {
        var element = $(this);
        var width = element.width();
        var elemWidth = 0;
        var elemPadding = 0;
        var config = [
                {width : 700,count : 5, padding : 0.03},
                {width : 560, count : 4, padding : 0.03},
                {width : 420, count : 3, padding : 0.03},
                {width : 280, count : 2, padding : 0.03},
                {width : 0,   count : 1, padding : 0.03}
            ];

        var i;
        for (i = 0; i < config.length; i++) {
            if (width >= config[i].width) {
                elemPadding = width * config[i].padding;
                elemWidth = width / config[i].count - elemPadding;
                element.data('carouselStep', config[i].count);
                // first wins, break now
                break;
            }
        }

        element.jcarousel('items').css('width', elemWidth + 'px').css('padding', '0 ' + elemPadding + 'px 0 0');
    };

    var initializePagingCarousel = function() {
        root.each(function() {
            var $container = $(this);
            var $carousel = $('.carousel', $container);
            var $counter = $('.carousel-counter', $container);

            var navigateCarousel = function(e, plusOrMinus) {
                e.preventDefault();
                $carousel.jcarousel('scroll', plusOrMinus + '=' + $carousel.data('carouselStep'));
                $('img.lazy', $carousel).trigger('appear');
            };
            $carousel
                .on('jcarousel:create jcarousel:reload', configurePagingCarousel)
                .on('jcarousel:createend', function() {
                    var carousel = $(this);
                    window.setTimeout(function(){
                        // re-apply lazyload after the creation of the carousel to elements visible
                        carousel.find('img.lazy:visible').lazyload();
                    }, 1000);
                })
                .on('jcarousel:createend', configurePagingCarousel)
                .jcarousel({wrap: 'circular'});
            $('.carousel-container .control-prev', $container).click(function(e) {
                navigateCarousel(e, '-');
            });
            $('.carousel-container .control-next', $container).click(function(e) {
                navigateCarousel(e, '+');
            });
        });
    };
    /* /PAGING CAROUSEL */

    var lazyLoadImages = function() {
        root.find('img.lazy').not('.lazy-marked').show().lazyload().addClass('lazy-marked');
        root.find('img.lazy').trigger('appear');
    };

    var loadRecommendations = function(url) {
        if (url !== null) {
            $.ajax({
                url: url
            }).done(function(html) {
                root.find('.product-recommendation-data').html(html);
                lazyLoadImages();
                initializePagingCarousel();
            }).fail(function() {
                root.remove();
            });
        }
    };

    loadRecommendations(root.data('baynote-service-url'));
    lazyLoadImages();
});

/** javascripts/bookmetrix.js **/
$(document).ready(function() {
  'use strict';

  var root = $('.book-metrics');

  var loadBookmetrix = function(url) {
    if(url !== null) {
      $.ajax({
        url: url
      }).done(function (html) {
        root.html(html);
      }).fail(function () {
        root.remove();
      });
    }
  };

  loadBookmetrix(root.data('bookmetrix-ajax-url'));
});

/** javascripts/vendor/webtrekk_v4.min.js **/
var webtrekkConfig = webtrekkConfig || {
    trackDomain: "springergmbh01.webtrekk.net",
    domain : "www.springer.com;checkout.springer.com;secure.worldpay.com",
    cookie : "1",
    contentId : "springer_com.other",
    executePluginFunction : "wt_socialMedia;wt_scrollposition"
};

/**
 ************ Don't change anything beyond this line ************
 ********************* Start webtrekk_v4.js *********************
 */
(function(k){k.webtrekkUnloadObjects=k.webtrekkUnloadObjects||[];k.webtrekkLinktrackObjects=k.webtrekkLinktrackObjects||[];k.webtrekkHeatmapObjects=k.webtrekkHeatmapObjects||[];var D=function(a){a.getJSON=function(a){if("string"===typeof a&&a&&"{"===a.charAt(0)&&"}"===a.charAt(a.length-1))try{var c=(new Function("return "+a))();if("object"===typeof c)return c}catch(b){}return null};a.parseJSON=function(d,c){for(var b in d){var f=b;if("object"===typeof d[f])"undefined"!==typeof a.jsonPara[f]&&"object"!==
typeof a.config[a.jsonPara[f][0]]&&(a.config[a.jsonPara[f][0]]={}),a.parseJSON(d[f],f);else if(c){if(isNaN(parseInt(f))||500>parseInt(f))a.config[a.jsonPara[c][0]][f]=d[f]}else"undefined"!==typeof a.jsonPara[f]&&(a.config[a.jsonPara[f][0]]=d[f])}};a.getMappingParam=function(d){var c=d.split(""),b,f,h;for(b=0;b<c.length;b++)if(!isNaN(parseInt(c[b]))){f=b;break}f?(c=d.substr(0,f),h=d.substr(f,d.length-1)):c=d;return{mapping:"undefined"!==typeof a.jsonPara[c]?a.jsonPara[c][0]:!1,index:h?h:!1}}},E=function(a){a.checkAsynchron=
function(a,c,b,f){if("undefined"!==typeof k[a])return c?c(!0,b):1;if(0>=f)return c?c(!1,b):1;k.setTimeout(function(){b.checkAsynchron(a,c,b,f-100)},100);return 1};a.loadAsynchron=function(d,c,b,f){a.include(d)&&a.checkAsynchron(c,b?b:!1,a,f?f:2E3)};a.include=function(a){if(!document.createElement)return!1;var c=document.getElementsByTagName("head").item(0),b=document.createElement("script");b.setAttribute("language","javascript");b.setAttribute("type","text/javascript");b.setAttribute("src",a);c.appendChild(b);
return!0}},F=function(a){a.registerEvent=function(d,c,b){d.addEventListener?("webkitvisibilitychange"===c&&a.unregisterEvent(d,c,b),d.addEventListener(c,b,!1)):d.attachEvent&&("beforeunload"!==c&&"webkitvisibilitychange"!==c||a.unregisterEvent(d,c,b),d.attachEvent("on"+c,b))};a.unregisterEvent=function(a,c,b){a.removeEventListener?a.removeEventListener(c,b,!1):a.detachEvent&&a.detachEvent("on"+c,b)}},G=function(a){a.getTimezone=function(){return Math.round(-1*((new Date).getTimezoneOffset()/60))};
a.maxlen=function(a,c){return a&&a.length>c?a.substring(0,c-1):a};a.indexOf=function(a,c,b){return a.indexOf(c,b?b:0)};a.wtTypeof=function(a){return"undefined"!==typeof a?1:0};a.wtLength=function(a){return"undefined"!==typeof a?a.length:0};a.wtEscape=function(a){try{return encodeURIComponent(a)}catch(c){return escape(a)}};a.wtUnescape=function(a){try{return decodeURIComponent(a)}catch(c){return unescape(a)}};a.getAttribute=function(a,c){return"string"===typeof a.getAttribute(c)?a.getAttribute(c):
"object"===typeof a.getAttribute(c)&&"object"===typeof a.attributes[c]&&null!==a.attributes[c]?a.attributes[c].nodeValue:""};a.plugInArray=function(a,c){if("object"!==typeof a)return!1;for(var b=0;b<a.length;b++){var f=RegExp(a[b].toLowerCase(),"g");if(-1!==c.toLowerCase().search(f))return a[b]}return!1}},H=function(a){a.decrypt=function(d){var c="";if(d)try{c=a.wtUnescape(d.replace(/([0-9a-fA-F][0-9a-fA-F])/g,"%$1"))}catch(b){}return c};a.checkSC=function(d){if("string"!==typeof a.secureConfig)return!1;
for(var c=a.secureConfig.split(";"),b=0;b<c.length;b++)if(c[b]===d)return!0;return!1}},I=function(a){a.wtHref=function(){return a.wtLocation().href};a.wtLocation=function(){return document.location};a.urlParam=function(d,c,b){if(!d||null===d||"undefined"===typeof d)return b;var f=[];0<d.indexOf("?")&&(f=d.split("?")[1].replace(/&amp;/g,"&").split("#")[0].split("&"));for(d=0;d<f.length;d++)if(0===f[d].indexOf(c+"="))return a.wtUnescape(f[d].substring(c.length+1).replace(/\+/g,"%20"));return b};a.isOwnDomain=
function(d){var c="";if(a.domain)if(0===a.domain.toUpperCase().indexOf("REGEXP:")){if(c=RegExp(a.domain.substring(7),"i"),c.test(a.getDomain(d)))return!0}else{c=a.domain.split(";");d=a.getDomain(d);for(var b=0;b<c.length;b++)if(d===c[b])return!0}return!1};a.getDomain=function(d){if("string"!==typeof d)return"";d=a.wtUnescape(d);d=d.split("://")[1];var c=RegExp("^(?:[^/]+://)?([^/:]+)","g");return"undefined"!==typeof d&&(d=d.match(c),d[0])?d[0].toLowerCase():""}},x=function(a,d,c){var b=RegExp(d+"="+
c);c=document.location.href;d=(new Date).getTime();b=b.exec(c);c=/wt_t=([\d]{13})/.exec(c);return b&&("undefined"!==typeof b[1]&&c&&"undefined"!==typeof c[1])&&parseInt(c[1])+9E5>d?a.wtUnescape(b[1]):!1},J=function(a){var d=!1;a.baseparams=function(){var c=screen.width+"x"+screen.height+",",c=c+(("undefined"!==typeof screen.colorDepth?screen.colorDepth:screen.pixelDepth)+","),b;b=navigator.cookieEnabled;b="boolean"!==typeof b?-1!==document.cookie.indexOf("="):b;c=c+(b?"1,":"0,")+((new Date).getTime()+
",");if(a.referrerOncePerSession&&a.getCookie("wt_ropc"))b="2";else{b="0";var f=x(a,"wt_ref","(.+?)(&|$)");""!==a.getCookie("wt_ref")?(b=a.wtEscape(a.getCookie("wt_ref")),a.setCookie("wt_ref","",-3600)):f?b=a.wtEscape(f):d?(b=a.wtEscape(d),d=!1):0<document.referrer.length&&(b=a.wtEscape(document.referrer));a.sentFullPixel?b="2":a.isOwnDomain(b)&&(b="1");a.referrerOncePerSession&&1<b.length&&a.setCookie("wt_ropc","1")}f=k.innerHeight;if(!f)try{f=document.documentElement.clientHeight}catch(h){}if(!f)try{f=
document.body.clientHeight}catch(g){}"undefined"===typeof f&&(f=-1);var e;e=k.innerWidth;if(!e)try{e=document.documentElement.clientWidth}catch(m){}if(!e)try{e=document.body.clientWidth}catch(p){}"undefined"===typeof e&&(e=-1);f&&f>screen.height&&(f=screen.height);e&&e>screen.width&&(e=screen.width);c=c+b+(","+e+"x"+f);return c+=","+(navigator.javaEnabled()?"1":"0")};a.setReferrer=function(a){"string"===typeof a&&(d=a)}},x=function(a,d,c){var b=RegExp(d+"="+c);c=document.location.href;d=(new Date).getTime();
b=b.exec(c);c=/wt_t=([\d]{13})/.exec(c);return b&&("undefined"!==typeof b[1]&&c&&"undefined"!==typeof c[1])&&parseInt(c[1])+9E5>d?a.wtUnescape(b[1]):!1},z=function(a,d,c,b){"1"!==a.cookie||(a.optOut||a.deactivatePixel)||a.firstParty();(c=c?c:"")||(c=a.formObject&&"noForm"!==d?"form":"link");"function"===typeof a.beforeUnloadPixel?a.beforeUnloadPixel():"form"===c&&a.executePlugin(a.getPluginConfig("form","before"));var f="";if(a.config.linkId&&(f+="&ct="+a.wtEscape(a.maxlen(a.wtUnescape(a.config.linkId),
255)))){a.linktrackOut&&(f+="&ctx=1");var h=a.ccParams;"string"===typeof h&&""!==h&&(f+=h)}if(a.wtEp)if(a.wtEpEncoded)f+=a.wtEp;else if(h=a.wtEp,"string"===typeof h&&""!==h)for(var h=h.split(/;/),g=0;g<h.length;g++)if("undefined"!==typeof h[g]){var e=h[g].split(/\=/);a.checkSC("custom")&&(e[1]=a.decrypt(e[1]));e[1]=a.wtEscape(e[1]);f+="&"+e[0]+"="+e[1]}"noForm"!==d&&(f+=b.getFormTrackingData());""!==f&&(a.quicksend(a.wtEscape(a.contentId.split(";")[0])+",1,"+a.baseparams(),f),a.config.linkId="",a.ccParams=
"",a.wtEp="");"function"===typeof a.afterUnloadPixel?a.afterUnloadPixel():"form"===c&&a.executePlugin(a.getPluginConfig("form","after"))},y=function(a){var d=[],c;for(c in a.safetagParameter){var b=c;if("executePluginFunction"===b)a.executePluginFunction+=a.safetagParameter[b],a.safetagParameter[b]="";else if("object"===typeof a.safetagParameter[b]){"object"!==typeof a[b]&&(a[b]={});for(var f in a.safetagParameter[b]){var h=f;a[b][h]=a.safetagParameter[b][h]}}else a[b]=a.safetagParameter[b],"linkTrack"!==
b&&"heatmap"!==b&&"tabBrowsing"!==b&&"execCDB"!==b&&"execRTA"!==b||d.push(b)}for(c=0;c<d.length;c++)switch(d[c]){case "linkTrack":a.linkTrackInit();break;case "heatmap":a.heatmapTrackInit();break;case "tabBrowsing":a.startTabBrowsing();break;case "execCDB":a.startCDB();break;case "execRTA":a.startRTA()}a.safetagParameter.pixel=a},K=function(a){var d=function(b,c,d){!1!==b?b():a.executePlugin(a.getPluginConfig(c?c:"page",d))};a.getMediaCode=function(){if(a.mediaCode){var b=[],c=a.mediaCode.split(";"),
d=0,g=[];a.mediaCodeValue&&(b=a.mediaCodeValue.split(";"));for(var e=0;e<c.length;e++){var m=a.urlParam(location.href,c[e],"");if(a.mediaCodeCookie){var k=!1,l=(a.trackId+"").split(",")[0],l="wt_mcc_"+(a.config.campaignAction?a.config.campaignAction.substring(0,1):"c")+"_"+l,n=a.getCookie(l),r;r=c[e]+"_"+m;for(var q=0,s=r.length,u=void 0,t=0;t<s;t++)u=r.charCodeAt(t),q=(q<<5)-q+u,q&=q;r=q>1E15-1?"0":q+"";-1===n.indexOf(","+r+",")&&m?(g.push(c[e]+a.wtEscape("="+m)),k=!0):d++;k&&(m="","eid"===a.mediaCodeCookie&&
(m=2592E3),a.setCookie(l,n+","+r+",",m))}else"undefined"!==typeof b&&"undefined"!==typeof b[e]&&""!==b[e]?g.push(c[e]+a.wtEscape("="+b[e])):""!==m&&g.push(c[e]+a.wtEscape("="+m))}c.length===d&&0!==c.length&&g.push("ignore%3Dignore");a.config.campaignId=g.join(";")}};a.getExtLifeCycles=function(b,c,d){for(var g="",e={},m=b.split("|"),k=0;k<m.length;k++){for(var l=m[k].split(";"),n=0;n<l.length;n++)g=0===n?g+a.wtEscape(l[n]):g+l[n],g+=";";g=g.substr(0,g.length-1);g+="|"}g=g.substr(0,g.length-1);e.xlcl=
a.wtEscape(b.split("|").length);e.xlct=a.wtEscape(c);"undefined"!==typeof d&&(e.xlcv=a.wtEscape(d));e.xlc=a.wtEscape(g);b="";for(var r in e)b+="&"+r+"="+e[r];return b};a.quicksend=function(b,c,d){!a.trackDomain||!a.trackId||a.deactivatePixel||a.deactivateRequest?a.deactivateRequest=!1:(d||(d="wt"+(a.fileSuffix?".pl":"")),"undefined"===typeof a.requestTimeout&&(a.requestTimeout=5),a.geid&&(c="&geid="+a.geid+c),a.cdbeid&&(c="&cdbeid="+a.cdbeid+c),"1"===a.cookie&&(c="&eid="+a.eid+"&one="+(a.cookieOne?
"1":"0")+"&fns="+(a.forceNewSession?"1":"0")+c),"1"!==a.cookie&&(a.cookieEidTimeout&&(c="&cet="+a.cookieEidTimeout+c),a.cookieSidTimeout&&(c="&cst="+a.cookieSidTimeout+c)),0<a.pixelSampling&&(c+="&ps="+a.pixelSampling),c="&tz="+a.getTimezone()+c,c+="&pu="+a.wtEscape(document.location.href.split("#")[0]),b="//"+a.trackDomain+"/"+a.trackId+"/"+d+"?p="+a.version+","+b+c+"&eor=1",a.tabBrowsing?a.sendTabBrowsingPixel(b,d):!a.ignorePrerendering&&a.isChrome&&"undefined"!==typeof document.webkitHidden?("object"!==
typeof a.prerendering&&(a.prerendering=[]),document.webkitHidden?(a.prerendering.push(b),a.registerEvent(document,"webkitvisibilitychange",function(){a.sendPrerendering()})):a.sendPixel(b,d)):a.sendPixel(b,d),"hm"!==d&&"hm.pl"!==d&&(a.cookieOne=!1,a.forceNewSession=!1,a.sentFullPixel=1))};a.sendPrerendering=function(){if(!document.webkitHidden){for(var b=0;b<a.prerendering.length;b++)a.sendPixel(a.prerendering[b]);a.prerendering=[]}};a.send=function(b,c,d){if("link"===c||"click"===c)a.config.linkId=
b;a.config.contentId=a.config.contentId?a.config.contentId:a.contentId;(b=b?b:a.config.contentId)||(b="no_content");b=a.wtEscape(b)+",1,";b+=a.baseparams();var g;g=[];if(navigator.plugins&&"Microsoft Internet Explorer"!==navigator.appName)for(var e=0,m=navigator.plugins.length;e<m;e++){var p="",p="Shockwave Flash"===navigator.plugins[e].name?navigator.plugins[e].description:navigator.plugins[e].name;(p=a.plugInArray(a.plugins,p))&&g.push(p)}g=g.join("|");e="";if(a.paramFirst)for(m=a.paramFirst.split(";"),
p=0;p<m.length;p++){var l=a.getMappingParam(m[p]),n=l.mapping,l=l.index;n&&(l?a.config[n]&&("undefined"!==typeof a.config[n][l]&&a.config[n][l])&&(e+="&"+m[p]+"="+a.wtEscape(a.config[n][l])):a.config[n]&&(e+="&"+m[p]+"="+a.wtEscape(a.config[n])))}if("string"===typeof d&&""!==d)for(var r=d.split(/;/),q=0;q<r.length;q++){if("undefined"!==typeof r[q]){var s=r[q].split(/\=/);a.checkSC("custom")&&(s[1]=a.decrypt(s[1]));s[1]=a.wtEscape(s[1]);e+="&"+s[0]+"="+s[1]}}else{a.wtEpEncoded=!1;l=new a.PredefinedParameter;
for(s in a.config)d=s+"",l.put(d,a.config[d]);var u=l.get("all");a.config.customParameter=a.mergeCustomParameter(a.config.customParameter,u.customParameter);s=a.checkCustomParameter(a.config.customParameter,"cp");a.config.customSessionParameter=a.mergeCustomParameter(a.config.customSessionParameter,u.customSessionParameter);d=a.checkCustomParameter(a.config.customSessionParameter,"cs");m=a.checkCustomParameter(a.config.customTimeParameter,"ce");p=a.checkCustomParameter(a.cdb,"cdb");a.config.customEcommerceParameter=
a.mergeCustomParameter(a.config.customEcommerceParameter,u.customEcommerceParameter);n=a.checkCustomParameter(a.config.customEcommerceParameter,"cb");a.config.orderValue&&-1===a.paramFirst.indexOf("ov;")&&(e=a.checkSC("order")?e+("&ov="+a.wtEscape(a.decrypt(a.config.orderValue))):e+("&ov="+a.wtEscape(a.config.orderValue)));a.config.currency&&-1===a.paramFirst.indexOf("cr;")&&(e=a.checkSC("order")?e+("&cr="+a.wtEscape(a.decrypt(a.config.currency))):e+("&cr="+a.wtEscape(a.config.currency)));a.config.orderId&&
-1===a.paramFirst.indexOf("oi;")&&(e+="&oi="+a.wtEscape(a.config.orderId));a.config.product&&(-1===a.paramFirst.indexOf("ba;")&&(e+="&ba="+a.wtEscape(a.config.product)),a.config.productCost&&-1===a.paramFirst.indexOf("co;")&&(e+="&co="+a.wtEscape(a.config.productCost)),a.config.productQuantity&&-1===a.paramFirst.indexOf("qn;")&&(e+="&qn="+a.wtEscape(a.config.productQuantity)),a.config.productCategory=a.mergeCustomParameter(a.config.productCategory,u.productCategory),e+=a.checkCustomParameter(a.config.productCategory,
"ca"),a.config.productStatus&&-1===a.paramFirst.indexOf("st;")&&(e+="&st="+a.wtEscape(a.config.productStatus)));var t=x(a,"wt_cd","(.+?)(&|$)");a.config.customerId||(a.config.customerId=t);a.config.customerId&&-1===a.paramFirst.indexOf("cd;")&&(e+="&cd="+a.wtEscape(a.config.customerId));a.config.crmCategory=a.mergeCustomParameter(a.config.crmCategory,u.crmCategory);e+=a.checkCustomParameter(a.config.crmCategory,"vc");null===l.get("birthday")&&(a.config.birthdayJ&&a.config.birthdayM&&a.config.birthdayD)&&
l.put("birthday",a.config.birthdayJ+a.config.birthdayM+a.config.birthdayD);null!==l.get("telefon")&&l.put("telefon",l.get("telefon").replace(/\W|_/g,""));a.config.urmCategory=a.mergeCustomParameter(a.config.urmCategory,u.urmCategory);e+=a.checkCustomParameter(a.config.urmCategory,"uc");a.browserLang&&(e+="&la="+a.wtEscape(a.browserLang));a.config.contentGroup=a.mergeCustomParameter(a.config.contentGroup,u.contentGroup);e+=a.checkCustomParameter(a.config.contentGroup,"cg");l="";if(a.config.campaignId){var t=
a.config.campaignAction?a.config.campaignAction.substring(0,1):"c",v=t+"_"+a.config.campaignId;v in a.sentCampaignIds?a.config.campaignId="ignore%3Dignore":a.sentCampaignIds[v]=!0;-1===a.paramFirst.indexOf("mc;")&&(e+="&mc="+a.wtEscape(a.config.campaignId));-1===a.paramFirst.indexOf("mca;")&&(e+="&mca="+t)}a.config.customCampaignParameter=a.mergeCustomParameter(a.config.customCampaignParameter,u.customCampaignParameter);l+=a.checkCustomParameter(a.config.customCampaignParameter,"cc");a.trackingSwitchMediaCode&&
(e+="&tmc="+a.wtEscape(a.trackingSwitchMediaCode));a.trackingSwitchMediaCodeValue&&(e+="&tmcv="+a.wtEscape(a.trackingSwitchMediaCodeValue));a.trackingSwitchMediaCodeTimestamp&&(e+="&tmct="+a.wtEscape(a.trackingSwitchMediaCodeTimestamp));"object"===typeof k.wt_ts&&("undefined"!==typeof k.wt_ts.trackingSwitchMediaCode&&(e+="&tmc="+a.wtEscape(k.wt_ts.trackingSwitchMediaCode)),"undefined"!==typeof k.wt_ts.trackingSwitchMediaCodeValue&&(e+="&tmcv="+a.wtEscape(k.wt_ts.trackingSwitchMediaCodeValue)),"undefined"!==
typeof k.wt_ts.trackingSwitchMediaCodeTimestamp&&(e+="&tmct="+a.wtEscape(k.wt_ts.trackingSwitchMediaCodeTimestamp)));u="";"undefined"!==typeof k.wt_vt&&(q=k.wt_vt);"undefined"===typeof q&&(q=a.urlParam(location.href,"wt_vt",!1));if(q)for(t=a.getCookie("wt_vt").split(";"),v=0;v<t.length;v++)-1!==t[v].indexOf(q+"v")&&(u="&wt_vt="+t[v].split("t")[0].split("v")[1]);u&&(e+=u);a.config.internalSearch&&-1===a.paramFirst.indexOf("is;")&&(e+="&is="+a.wtEscape(a.maxlen(a.wtUnescape(a.config.internalSearch),
255)));if(a.config.dynamicParameters)for(r in a.config.dynamicParameters)q=r+"",a.config.dynamicParameters[q]&&(e="object"===typeof a.config.dynamicParameters[q]?e+a.checkCustomParameter(a.config.dynamicParameters,q):e+("&"+r+"="+a.wtEscape(a.config.dynamicParameters[q])));s&&(e+=s);l&&(e+=l);m&&(e+=m);n&&(e+=n);d&&(e+=d);p&&(e+=p);a.config.customSid&&(e+="&csid="+a.config.customSid);a.config.customEid&&(e+="&ceid="+a.config.customEid);a.config.xwtip&&(e+="&X-WT-IP="+a.wtEscape(a.config.xwtip));a.config.xwtua&&
(e+="&X-WT-UA="+a.wtEscape(a.config.xwtua));a.config.xwtrq&&(e+="&X-WT-RQ="+a.wtEscape(a.config.xwtrq));a.xwteid&&(e+="&X-WT-EID="+a.wtEscape(a.xwteid),a.xwteid=!1);a.config.xwtstt&&(e+="&X-WT-STT="+a.wtEscape(a.config.xwtstt));!a.sentFullPixel&&a.firstVisitContact&&(e+="&fvc="+a.firstVisitContact);!a.sentFullPixel&&a.lastVisitContact&&(e+="&lvc="+a.lastVisitContact)}a.config.linkId&&a.config.customClickParameter&&(e+=a.checkCustomParameter(a.config.customClickParameter[a.config.linkId]?a.config.customClickParameter[a.config.linkId]:
a.config.customClickParameter,"ck"),a.ccParams=!1);a.config.xlc&&a.config.xlct&&(""!==a.config.xlc||""!==a.config.xlct)&&(r="",r=a.config.xlcv?a.getExtLifeCycles(a.config.xlc,a.config.xlct,a.config.xlcv):a.getExtLifeCycles(a.config.xlc,a.config.xlct),e+=r);a.config.contentId||a.config.linkId||(a.config.contentId=a.contentId,a.config.linkId="wt_ignore");a.config.linkId?(a.wtEp=e,a.wtEpEncoded=!0,z(a,"noForm",c)):("1"===a.cookie?a.cookieOne&&(e+="&np="+a.wtEscape(g)):e+="&np="+a.wtEscape(g),a.quicksend(b,
e))};var c=function(){a.safetagTimeoutStarted=!0;var b=(new Date).getTime()-a.startSafetagTimeoutDate;if(a.safetagInProgress&&b<a.safetag.timeout)k.setTimeout(function(){c()},5);else{a.safetagTimeoutStarted=!1;a.safetagInProgress=!1;b>a.safetag.timeout&&(a.xwtstt=a.safetag.timeout+"");for(b=0;b<a.saveSendinfoArguments.length;b++){var d=a.saveSendinfoArguments[b];a.sendinfo(d[0],d[1],d[2],d[3])}a.saveSendinfoArguments=[]}};a.sendinfo=function(b,f,h,g){h=h?h:"page";-1===location.href.indexOf("fb_xd_fragment")&&
(a.safetag&&y(a),a.config="object"===typeof b?b:a.getConfig(),a.safetagInProgress?(b?a.saveSendinfoArguments.push([a.config,f,h,g]):a.saveSendinfoArguments.push([!1,f,h,g]),a.safetagTimeoutStarted||(a.startSafetagTimeoutDate=(new Date).getTime(),k.setTimeout(function(){c()},5))):(a.config.linkId&&(h="click",f||(f=a.config.linkId)),d(a.beforeSendinfoPixel,h,"before"),a.safetag&&y(a),a.optOut||a.deactivatePixel||("1"===a.cookie?a.firstParty():a.xwteid=x(a,"wt_eid","([\\d]{19})")),a.config.campaignId||
(!a.mediaCode||"page"!==h||a.config.linkId)||a.getMediaCode(),""===a.contentId&&""===f||a.send(f,h,g),d(a.afterSendinfoPixel,h,"after")))}},L=function(a){for(var d=[D,E,F,G,H,I,J,K],c=0;c<d.length;c++)d[c](a)},M=function(a){a.setProperty("paramFirst","");a.jsonPara={ck:["customClickParameter",{}],cp:["customParameter",{}],cs:["customSessionParameter",{}],ce:["customTimeParameter",{}],cb:["customEcommerceParameter",{}],vc:["crmCategory",{}],uc:["urmCategory",{}],ca:["productCategory",{}],cc:["customCampaignParameter",
{}],cg:["contentGroup",{}],ct:["linkId",""],ov:["orderValue",""],cr:["currency",""],oi:["orderId",""],ba:["product",""],co:["productCost",""],qn:["productQuantity",""],st:["productStatus",""],cd:["customerId",""],is:["internalSearch",""],mc:["campaignId",""],mca:["campaignAction",""]};a.checkBrowser=function(){a.isIE=-1!==navigator.appName.indexOf("Microsoft");a.isIE||(a.isOpera=-1!==navigator.appName.indexOf("Opera"),a.isOpera||(a.isSafari=-1!==navigator.vendor.toLowerCase().indexOf("apple"),a.isChrome=
-1!==navigator.vendor.toLowerCase().indexOf("google"),a.isSafari||a.isChrome||(a.isFirefox=-1!==navigator.userAgent.toLowerCase().indexOf("firefox"))))};a.generateDefaultConfig("trackId trackDomain domain forceHTTPS secureConfig beforeSendinfoPixel afterSendinfoPixel beforeUnloadPixel afterUnloadPixel ignorePrerendering plugins trackingSwitchMediaCode trackingSwitchMediaCodeValue trackingSwitchMediaCodeTimestamp isIE isOpera isSafari isChrome isFirefox wtEpEncoded fileSuffix dynamicParameters xwtip xwtua xwtrq config xwteid xwtstt customTimeParameter sentFullPixel wtEp".split(" "));
a.plugins||(a.plugins="Adobe Acrobat;Windows Media Player;Shockwave Flash;RealPlayer;QuickTime;Java;Silverlight".split(";"));"string"===typeof a.plugins&&(a.plugins=a.plugins.split(";"));a.browserLang=!1;"string"===typeof navigator.language?a.browserLang=navigator.language.substring(0,2):"string"===typeof navigator.userLanguage&&(a.browserLang=navigator.userLanguage.substring(0,2));a.checkBrowser()},N=function(a){a.url2contentId=function(a){if(!a)return"no_content";a=/\/\/(.*)/.exec(a);return 1>a.length?
"no_content":a[1].split("?")[0].replace(/\./g,"_").replace(/\//g,".").replace(/\.{2,}/g,".").toLowerCase().split(";")[0]};a.generateDefaultConfig("contentId contentGroup internalSearch numberSearchResults errorMessages npsScore npsScoreFeedback paywall articleTitle pageMainCategory pageSubCategory pageAuthor contentTags pageTitle pageType pageLength daysSincePublication testVariant testExperiment customParameter".split(" "));a.contentId=a.contentId?a.contentId:a.url2contentId(document.location.href)},
O=function(a){"undefined"===typeof a.safetag&&(a.safetag=!1);"undefined"===typeof a.safetagInProgress&&(a.safetagInProgress=!1);"undefined"===typeof a.safetagParameter&&(a.safetagParameter={});"undefined"===typeof a.update&&(a.update=function(){});a.saveSendinfoArguments=[];a.safetagTimeoutStarted=!1},P=function(a){a.generateDefaultConfig(["loginStatus","pixelVersion","trackingPlatform","customSessionParameter","forceNewSession"])},Q=function(a){a.generateDefaultConfig("customEcommerceParameter orderValue currency orderId product productCost productQuantity productCategory productStatus couponValue productLabel productMainCategory productSubCategory productManufacturer paymentMethod productShortDescription productLongDescription shippingService shippingSpeed shippingCosts grossMargin orderStatus productVariant productSoldOut".split(" "))},
R=function(a){a.generateDefaultConfig("crmCategory urmCategory customerId customSid customEid xlc xlct xlcv paywallUser email emailRID emailOptin firstName lastName telefon gender birthday birthdayJ birthdayM birthdayD country city postalCode street streetNumber validation".split(" "))},S=function(a){a.setProperty("formAttribute","name");a.setProperty("formFieldAttribute","name");a.setProperty("formValueAttribute","value");a.setProperty("formFieldDefaultValue",{});a.setProperty("multipleFormArray",
[]);a.generateDefaultConfig("form formFullContent formAnonymous gatherFormsP formObject formName formFocus formSubmit formPathAnalysis".split(" "))},T=function(a){a.setProperty("linkTrackAttribute","name");a.setProperty("delayLinkTrackTime",200);a.generateDefaultConfig("linkId linkTrack linkTrackParams linkTrackPattern linkTrackReplace linkTrackDownloads linkTrackIgnorePattern customClickParameter delayLinkTrack noDelayLinkTrackAttribute linktrackOut linktrackNamedlinksOnly ccParams".split(" "))},
U=function(a){a.sentCampaignIds={};a.setProperty("campaignAction","click");a.generateDefaultConfig("mediaCode mediaCodeValue mediaCodeCookie campaignId customCampaignParameter referrerOncePerSession".split(" "))},V=function(a){a.cdb={};a.cdbData=[]},W=function(a,d,c){a.getConfig=function(b){var c={},d;for(d in a)c[d]=b?!1:a[d];return c};a.setProperty=function(b,f){a[b]=f;"undefined"!==typeof d[b]?a[b]=d[b]:"undefined"!==typeof c[b]&&(a[b]=c[b])};a.generateDefaultConfig=function(b){for(var c=0;c<b.length;c++)a.setProperty(b[c],
!1)};(function(){for(var b=[M,N,O,P,Q,R,S,T,U,V],f=0;f<b.length;f++)b[f](a,d,c)})()},y=function(a){var d=[],c;for(c in a.safetagParameter){var b=c;if("executePluginFunction"===b)a.executePluginFunction+=a.safetagParameter[b],a.safetagParameter[b]="";else if("object"===typeof a.safetagParameter[b]){"object"!==typeof a[b]&&(a[b]={});for(var f in a.safetagParameter[b]){var h=f;a[b][h]=a.safetagParameter[b][h]}}else a[b]=a.safetagParameter[b],"linkTrack"!==b&&"heatmap"!==b&&"tabBrowsing"!==b&&"execCDB"!==
b&&"execRTA"!==b||d.push(b)}for(c=0;c<d.length;c++)switch(d[c]){case "linkTrack":a.linkTrackInit();break;case "heatmap":a.heatmapTrackInit();break;case "tabBrowsing":a.startTabBrowsing();break;case "execCDB":a.startCDB();break;case "execRTA":a.startRTA()}a.safetagParameter.pixel=a},X=function(a){a.pageCounter=0;a.clickCounter=0;a.linkCounter=0;a.formCounter=0;a.heatmapCounter=0;a.configCounter=0;a.plugin={};a.setProperty("executePluginFunction","");a.getRequestCounter=function(d,c){var b=0;"after"!==
c&&b++;return"undefined"!==typeof a[d+"Counter"]?(a[d+"Counter"]+=b,a[d+"Counter"]):0};a.getPluginConfig=function(d,c){return{instance:a,mode:d,type:c,requestCounter:a.getRequestCounter(d,c)}};a.executePlugin=function(d,c){if(!a.executePluginFunction||"string"!==typeof a.executePluginFunction)return!1;var b=c?c:{};a.epf=!1;for(var f=a.executePluginFunction.split(";"),h=0;h<f.length;h++)f[h]&&"function"===typeof k[f[h]]&&(a.epf=k[f[h]],a.epf(d,b));return!1!==a.epf};a.triggerConfigPlugins=function(d){"undefined"!==
typeof a.safetag&&a.safetag&&y(a);a.config="object"===typeof d?d:a.getConfig();a.executePlugin(a.getPluginConfig("config","before"));"undefined"!==typeof a.safetag&&a.safetag&&y(a);a.executePlugin(a.getPluginConfig("config","after"))}},Y=function(a){a.setProperty("cookie","3");a.setProperty("updateCookie",!0);a.setProperty("cookieSecure",!1);a.setProperty("cookieEidTimeout",!1);a.setProperty("cookieSidTimeout",!1);var d=function(c,b,d,h){c=a.wtEscape(c)+"="+a.wtEscape(b);c=c+(";domain="+d)+";path=/";
h&&(c+=";expires="+a.getExpiryDate(h));a.cookieSecure&&(c+=";secure");document.cookie=c};a.getExpiryDate=function(a){var b=new Date,d=b.getTime();b.setTime(d+6E4*a);return b.toUTCString()};a.setCookie=function(c,b,f){var h=document.location.hostname;if(-1===h.search(/^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/))for(var g=h.split("."),e=g[g.length-1],m=g.length-2;0<=m;m--)if(e=g[m]+"."+e,d(c,b,e,f),a.getCookie(c)===b)return;d(c,b,h,f)};a.getCookie=function(c){for(var b=document.cookie.split(";"),
d=0;d<b.length;d++){var h=b[d].substr(0,b[d].indexOf("=")),g=b[d].substr(b[d].indexOf("=")+1),h=h.replace(/^\s+|\s+$/g,"");if(h===c)return a.wtUnescape(g)}return""}},Z=function(a){a.cookieManager=function(a,c,b,f){this.name=a;this.keySeperator="~";this.fieldSeparator="#";this.durationSeperator="|";this.found=!1;this.expires=c?c:!1;this.accessPath=b?b:"/";this.secure=f?f:!1;this.rawValue="";this.fields=[];this.fieldsDuration=[];var h=function(a){try{return decodeURIComponent(a)}catch(b){return k.unescape(a)}},
g=function(a){try{return encodeURIComponent(a)}catch(b){return k.escape(a)}};this.read=function(){this.rawValue=null;this.found=!1;for(var a=document.cookie.split(";"),b=0;b<a.length;b++){var c=a[b].substr(0,a[b].indexOf("=")),d=a[b].substr(a[b].indexOf("=")+1),c=c.replace(/^\s+|\s+$/g,"");c===this.name&&(this.rawValue=d,this.found=!0)}if(null!==this.rawValue){a=b=0;do a=this.rawValue.indexOf(this.fieldSeparator,b),-1!==a&&(b=this.rawValue.substring(b,a).split(this.durationSeperator),c=b[0].split(this.keySeperator),
this.fields[c[0]]=h(c[1]),this.fieldsDuration[c[0]]=parseInt(h(b[1])),b=a+1);while(-1!==a&&a!==this.rawValue.length-1)}return this.found};this.getSize=function(){var a=(new Date).getTime(),b="",c;for(c in this.fields){var d=c+"";this.fieldsDuration[d]>=a&&(b+=g(d)+this.keySeperator+g(this.fields[d])+this.durationSeperator+g(this.fieldsDuration[d])+this.fieldSeparator)}return b.length};this.write=function(){var a=(new Date).getTime(),b=!0,c=this.name+"=",d;for(d in this.fields){var f=d+"";this.fieldsDuration[f]>=
a&&(c+=g(f)+this.keySeperator+g(this.fields[f])+this.durationSeperator+g(this.fieldsDuration[f])+this.fieldSeparator,b=!1)}a=b?-99999:this.expires;""!==a&&"number"===typeof a&&(b=new Date,b.setTime((new Date).getTime()+864E5*a),c+="; expires="+b.toGMTString());null!==this.accessPath&&(c+="; PATH="+this.accessPath);a=location.hostname;-1===a.search(/^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$/)&&(a=location.hostname.split("."),a=a[a.length-2]+"."+a[a.length-1]);b="";this.secure&&(b=";secure");
document.cookie=c+("; DOMAIN="+a)+b};this.remove=function(){this.expires=-10;this.write();return this.read()};this.get=function(a){var b=(new Date).getTime();return this.fieldsDuration[a]>=b?this.fields[a]:""};this.set=function(a,b,c,d,f){c=c?c:31536E3;d=d?d:"last";var h=(new Date).getTime();if("first"===d&&""!==this.fields[a]&&null!==this.fields[a]&&this.fieldsDuration[a]>=h)return this.fields[a];this.fields[a]=b;this.fieldsDuration[a]=h+1E3*parseInt(c);f||this.write();return b};this.prepare=function(a,
b,c,d){return this.set(a,b,c,d,!0)};this.read()}},$=function(a,d,c){Y(a,d,c);Z(a)},aa=function(a){a.setProperty("pixelSampling",!1);a.setPixelSampling=function(d){d=d?d:a.pixelSampling;for(var c=a.trackId.split(",")[0],b=a.getCookie("wt3_sample").split(";"),f=!1,h=0;h<b.length;h++)-1!==b[h].indexOf(c+"|"+d)?f=!0:-1!==b[h].indexOf(c+"|")&&(b[h]="");h=6;a.cookieEidTimeout&&(h=a.cookieEidTimeout);f?(b=b.join(";"),a.setCookie("wt3_sample",b,43200*h)):(Math&&Math.random&&0===parseInt(Math.random()*d)?
b.push(c+"|"+d+"|1"):b.push(c+"|"+d+"|0"),a.setCookie("wt3_sample",b.join(";"),43200*h),b=a.getCookie("wt3_sample"));-1===b.indexOf(c+"|"+d+"|1")&&(a.deactivatePixel=!0)};a.pixelSampling&&!a.optOut&&a.setPixelSampling()},ba=function(a,d,c){a.deactivatePixel=!1;a.optOut=!1;a.setProperty("optoutName","webtrekkOptOut");a.optOut=""!==a.getCookie(a.optoutName);a.optOut&&(a.deactivatePixel=!0);aa(a,d,c)},ca=function(a){a.deactivateRequest=!1;a.completeRequest=!1;a.setProperty("simulatedRequestTimeout",
500);a.setProperty("replaceMutatedVowel",!1);a.setProperty("requestLimitAmount",1E3);a.setProperty("requestLimitTime",1800);a.setProperty("maxRequestLength",7168);var d=!1;a.sendPixel=function(c,b){var f=c,h=-1!==f.indexOf("&ct=")?"link":-1!==f.indexOf("&fn=")?"form":-1!==f.indexOf("&mi=")?"media":-1!==f.indexOf("/hm")?"heatmap":-1!==f.indexOf("/cdb")||-1!==f.indexOf("/fbc")?"cdb":-1!==f.indexOf("/ce")?"independent":-1!==f.indexOf("/re")?"redirect":"page",g;a:{var e=a.trackId.split(",")[0],k=a.getCookie("wt_rla").split(";");
g={};for(var p=0,l=k.length;p<l;p++){var n=k[p].split(",");if(3<=n.length){var r=parseInt(n[1]),q=parseInt(n[2]);g[n[0]]={amount:isNaN(r)?0:r,time:isNaN(q)?(new Date).getTime():q}}}"undefined"===typeof g[e]&&(g[e]={amount:0,time:(new Date).getTime()});k=(new Date).getTime();if(k-g[e].time>1E3*a.requestLimitTime)d=!1,g[e]={amount:0,time:k};else if(g[e].amount===a.requestLimitAmount-1&&(d=!0),g[e].amount>=a.requestLimitAmount){g=!0;break a}if("link"===h||"page"===h||"form"===h){g[e].amount++;var e=
[],s;for(s in g)e.push(s+","+g[s].amount+","+g[s].time);a.setCookie("wt_rla",e.join(";"),86400)}g=!1}if(!g){if(a.replaceMutatedVowel){g=[[/%C3%84/g,"ae"],[/%C3%A4/g,"ae"],[/%C4/g,"ae"],[/%E4/g,"ae"],[/%C3%96/g,"oe"],[/%C3%B6/g,"oe"],[/%D6/g,"oe"],[/%F6/g,"oe"],[/%C3%9C/g,"ue"],[/%C3%BC/g,"ue"],[/%DC/g,"ue"],[/%FC/g,"ue"],[/%C3%9F/g,"ss"],[/%DF/g,"ss"]];s=0;for(e=g.length;s<e;s++)f=f.replace(g[s][0],g[s][1]);f=f.toLowerCase()}a.completeRequest=f.split("?")[1];f=a.maxlen(f,a.maxRequestLength);d&&(f+=
"&rla="+a.requestLimitAmount+"%7C"+a.requestLimitTime);!a.isFirefox||"link"!==h&&"form"!==h?a.sendPixelImage(f,b,h):a.sendPixelElement(f,b,h)}};a.sendPixelImage=function(c,b,d){var h="https:"===k.document.location.protocol?"https:":"http:";a.forceHTTPS&&(h="https:");0!==c.search(/https:|http:/)&&(c=h+c);if("hm"===b||"hm.pl"===b)c+="&hm_ts="+(new Date).getTime();"function"!==typeof k.Image&&(k.Image=function(){return document.createElement("img")});b=new k.Image;(function(b,c,d){var f=!1,h=function(d,
h){if(!f){f=!0;var k=(new Date).getTime()-c;b&&d&&a.executePlugin({instance:a,mode:h,type:d,requestCounter:0},{status:d,time:k,request:b.src,item:b})}};b.onerror=function(){h("error",d)};b.onload=function(){h("success",d)};k.setTimeout(function(){h("timeout",d)},a.simulatedRequestTimeout)})(b,(new Date).getTime(),d);b.src=c};a.createPixelElement=function(a){a=a.replace(/'/g,"%27");var b=document.createElement("div");b.style.width="0px";b.style.height="0px";b.style.backgroundImage='url("'+a+'")';k.setTimeout(function(){b.parentElement.removeChild(b)},
5E3);return b};a.sendPixelElement=function(c,b,d){var h="https:"===k.document.location.protocol?"https:":"http:";a.forceHTTPS&&(h="https:");0!==c.search(/https:|http:/)&&(c=h+c);if("hm"===b||"hm.pl"===b)c+="&hm_ts="+(new Date).getTime();if("undefined"===typeof a.sendPixelObject||null===a.sendPixelObject)(b=document.getElementById("webtrekk-image"))&&null!==b?a.sendPixelObject=b:(a.sendPixelObject=document.createElement("div"),a.sendPixelObject.id="webtrekk-image",a.sendPixelObject.style.width="0px",
a.sendPixelObject.style.height="0px",a.sendPixelObject.style.overflow="hidden",b=null,"object"===typeof document.body?b=document.body:"object"===typeof document.getElementsByTagName("body")[0]&&(b=document.getElementsByTagName("body")[0]),b&&null!==b?b.appendChild(a.sendPixelObject):(a.sendPixelObject=null,a.sendPixelImage(c,"wt"+(a.fileSuffix?".pl":""),d)));null!==a.sendPixelObject&&a.sendPixelObject.appendChild(a.createPixelElement(c))}},x=function(a,d,c){var b=RegExp(d+"="+c);c=document.location.href;
d=(new Date).getTime();b=b.exec(c);c=/wt_t=([\d]{13})/.exec(c);return b&&("undefined"!==typeof b[1]&&c&&"undefined"!==typeof c[1])&&parseInt(c[1])+9E5>d?a.wtUnescape(b[1]):!1},A=function(a){var d=a.trackId.split(",")[0],c="number"===typeof a.cookieEidTimeout?a.cookieEidTimeout:6,b=a.generateEid(),f="",h="",g=[],e=-1,m="",p=!1,l=b,n=!1,r=function(){c?a.setCookie("wt3_eid",g.join(";"),43200*c):a.setCookie("wt3_eid",g.join(";"))},q=function(b,c){for(var d=!1,f=a.getCookie("wt3_eid").split(";"),h=0,e=
f.length;h<e;h++)if(-1!==f[h].indexOf(c+"|")){d=!0;f[h]=c+"|"+b;break}d||f.push(c+"|"+b);g=f;r()},s=function(a){if(a&&"2"===a.substring(0,1)){a=parseInt(a.substring(1,11)+"000");a=new Date(a);var b=a.getFullYear()+"",b=b+(10>a.getMonth()+1?"0":""),b=b+(a.getMonth()+1),b=b+(10>a.getDate()?"0":""),b=b+a.getDate(),b=b+(10>a.getHours()?"0":""),b=b+a.getHours(),b=b+(10>a.getMinutes()?"0":"");return b+=a.getMinutes()}return""};this.init=function(){g=a.getCookie("wt3_eid").split(";");for(var c=0;c<g.length;c++)if(-1!==
g[c].indexOf(d+"|")){e=c;h=g[c];m=h.replace(d+"|","").split("#")[0];-1!==h.indexOf("#")&&(l=h.replace(d+"|","").split("#")[1]);break}f=x(a,"wt_eid","([\\d]{19})");"string"===typeof k.webtrekkApplicationEverId&&(f=k.webtrekkApplicationEverId);m?(f&&(g[e]=d+"|"+f,m=f,a.updateCookie=!0),"undefined"!==typeof k.wt_mcp_eid&&(p=!0),-1===g[e].indexOf("#")?g[e]+="#"+b:g[e]=h.replace(/#[0-9]{19}/g,"#"+b),a.updateCookie?r():n=l=!1):("string"===typeof k.wt_mcp_eid&&-1!==k.wt_mcp_eid.indexOf(d+"|")?(m=k.wt_mcp_eid.replace(d+
"|",""),p=!0):f?m=f:(m=a.generateEid(),p=!0),g.push(d+"|"+m+"#"+b),r());a.updateCookie&&(l=s(l),n=s(m))};this.getId=function(){var b;if(b=a.validateEverId)b=m.replace(/^\d((\d){10})\d+$/,"$1"),b=!(1089676800<parseInt(b));b&&(m=a.generateEid(),q(m,d));return m};this.setId=function(a,b){q(a,b)};this.getUrlId=function(){return f};this.getLastVisitContact=function(){return l};this.getFirstVisitContact=function(){return n};this.getCookieOne=function(){return p}},B=function(a){var d=a.trackId.split(",")[0],
c=[],b="";this.init=function(){c=a.getCookie("wt3_sid").split(";");for(var f=0;f<c.length;f++)if(-1!==c[f].indexOf(d)){b=c[f];break}b||c.push(d);a.setCookie("wt3_sid",c.join(";"))};this.getId=function(){return b};this.setId=function(b){for(var c=!1,d=a.getCookie("wt3_sid").split(";"),e=0,k=d.length;e<k;e++)if(-1!==d[e].indexOf(b)){c=!0;break}c||d.push(b);a.setCookie("wt3_sid",d.join(";"))}},ea=function(a){a.geid=!1;a.setProperty("execRTA",!0);var d=!1,c=function(){d=!0;if(!a.deactivatePixel){"string"!==
typeof a.execRTA&&(a.execRTA=document.location.protocol+"//cdn.wbtrk.net/js/geid.min.js");var b=a.getCookie("wt_geid");b?"68934a3e9455fa72420237eb"!==b&&(a.geid=b):a.loadAsynchron(a.execRTA,"wt_geid",function(a,b){a&&k.wt_geid(b,k)},2E3)}};a.execRTA&&c();a.startRTA=function(){d||(a.execRTA=a.execRTA?a.execRTA:!0,c())}},fa=function(a){var d="https:"===k.document.location.protocol?"https:":"http:",c=!1,b=function(a){var b=document.createElement("canvas"),c=null;try{c=b.getContext(a)}catch(d){}return c&&
null!==c},f=function(a,b){var c=document.createElement("img");c.crossOrigin="use-credentials";(function(a,b){var c=!1,d=function(d,f){if(!c){c=!0;if(d){var h=document.createElement("canvas"),e=h.getContext("2d");h.height=a.height;h.width=a.width;e.drawImage(a,0,0);try{var g=e.getImageData(0,0,6,1).data;return b(g)}catch(k){return b([],"5")}}return b([],f)}return 0};a.onerror=function(){d(!1,"4")};a.onload=function(){d(!0)};k.setTimeout(function(){d(!1,"3")},2E3)})(c,b);c.src=d+"//"+a},h=function(a){for(var b=
"",c=0,d;c<a.length;c++)d=a[c].toString(16),2>d.length&&(d="0"+d),b+=d,2===c%4&&c++;b=b.substr(0,b.length-4);return"string"===typeof b&&-1!==b.search(/^[0-9a-f]{32}$/)&&-1===b.search(/^[f]{32}$/)?b:""},g=function(b){"1"===b?a.setCookie("wt_cdbeid",b,15):a.setCookie("wt_cdbeid",b)},e=function(b){a.sendPixel(b,"")},m=function(b){("1"!==a.cookie||"1"===a.cookie&&a.eid)&&0<a.pageCounter?k.setTimeout(function(){b()},300):k.setTimeout(function(){m(b)},50)},p=function(b){m(function(){var c;if("1"===a.cookie)c=
"//fbc.wcfbc.net/v1/fbc"+("?p="+a.version+",0"),c+="&eid="+a.wtEscape(a.eid),c+="&acc="+a.wtEscape(a.trackId),c+="&t="+(new Date).getTime(),c+="&err="+b,e(c);else for(var f=a.trackId.split(","),h=0,g=f.length;h<g;h++){var t=d+"//fbc.wcfbc.net/v1/fbc",t=t+("?p="+a.version+",0"),t=t+("&acc="+a.wtEscape(f[h])),t=t+("&t="+(new Date).getTime()),t=t+("&err="+b);c="//";c+=a.trackDomain+"/"+a.trackId+"/cc";c+="?a=r&c=wteid_"+f[h];c+="&t="+a.wtEscape(t);e(c)}})},l=function(){c=!0;if(!a.deactivatePixel){var d=
a.getCookie("wt_cdbeid");d&&-1!==d.search(/^[0-9a-f]{32}$/)&&(a.cdbeid=d);d||(a.useCDBCache&&!a.isIE&&b("2d")?("string"!==typeof a.useCDBCache&&(a.useCDBCache="fbc.wcfbc.net/v1/fbc"),f(a.useCDBCache,function(b,c){var d=h(b);d?(g(d),m(function(){var b="//"+a.trackDomain+"/"+a.trackId+"/cdb",b=b+("?p="+a.version+",0,,,,,"+(new Date).getTime()+",,,");a.eid&&(b+="&eid="+a.eid);b+="&cdbeid="+d;e(b)})):(g("1"),p(c?c:"6"))})):(d="",a.useCDBCache&&a.isIE?d="1":a.useCDBCache&&!b("2d")&&(d="2"),g("1"),p(d)))}};
a.cdbeid=!1;a.setProperty("execCDB",!0);a.setProperty("useCDBCache",!1);a.startCDB=function(){c||(a.execCDB=!0,l())};a.execCDB&&l()},ga=function(a,d,c){a.eid=!1;a.firstVisitContact=!1;a.lastVisitContact=!1;a.cookieOne=!1;a.setProperty("globalVisitorIds",!1);a.setProperty("validateEverId",!1);a.zeroPad=function(a,c){for(var d="",g=0;g<=c;g++)d+="0";d+=a;return d.substring(d.length-c,d.length)};a.generateEid=function(){var b=Math.floor((new Date).getTime()/1E3);1089676800>b&&(b="3"+a.zeroPad(Math.floor(1E4*
Math.random()),4)+a.zeroPad(Math.floor(1E5*Math.random()),5));return"2"+a.zeroPad(b,10)+a.zeroPad(Math.floor(1E8*Math.random()),8)};a.setEverId=function(b,c){b&&("string"===typeof b&&-1!==b.search(/^[0-9]{19}$/))&&(c&&"string"===typeof b||(c=a.trackId.split(",")[0]),(new A(a)).setId(b,c),(new B(a)).setId(c))};a.firstParty=function(){var b=new A(a);b.init();var c=new B(a);c.init();a.eid=b.getId();a.cookieOne=b.getCookieOne();c.getId()||(b.getId()!==b.getUrlId()&&(a.forceNewSession=!0),a.firstVisitContact=
b.getFirstVisitContact(),a.lastVisitContact=b.getLastVisitContact())};a.globalVisitorIds&&(d.execRTA=!0,d.execCDB=!0);ea(a,d,c);fa(a,d,c)},ha=function(a){a.overlayOn="1"===a.urlParam(a.wtHref(),"wt_overlay","0")||"1"===a.getCookie("wt_overlay");var d=function(){"undefined"!==typeof k.wt_overlay?k.setTimeout(function(){k.wt_overlay()},1E3):("undefined"===typeof k.wt_overlay_retry&&(k.wt_overlay_retry=0),k.wt_overlay_retry++,60>k.wt_overlay_retry&&k.setTimeout(function(){d()},1E3))};"0"===a.urlParam("wt_overlay")&&
(a.overlayOn=!1,a.setCookie("wt_overlay","",-3600));a.overlayOn&&!a.disableOverlayView&&(a.setCookie("wt_overlay","1"),a.startHeatmapOrOverlay("overlay",d))},ia=function(a){a.heatmapOn="1"===a.urlParam(a.wtHref(),"wt_heatmap","0");a.setProperty("heatmapRefpoint","wt_refpoint");a.setProperty("heatmap",!1);var d=function(b){var c,d,g=document.getElementById(a.heatmapRefpoint);d=g&&null!==g?c=0:c=-1;if(g&&null!==g&&"undefined"!==typeof g.offsetLeft)for(;g;)c+=0<=g.offsetLeft?g.offsetLeft:0,d+=0<=g.offsetTop?
g.offsetTop:0,g=g.offsetParent;var e=g=0;b||(b=k.event);if(b.pageX||b.pageY)g=b.pageX,e=b.pageY;else if(b.clientX||b.clientY)if(g=b.clientX,e=b.clientY,a.isIE)if(0<document.body.scrollLeft||0<document.body.scrollTop)g+=document.body.scrollLeft,e+=document.body.scrollTop;else if(0<document.documentElement.scrollLeft||0<document.documentElement.scrollTop)g+=document.documentElement.scrollLeft,e+=document.documentElement.scrollTop;b=0;b=a.isIE?document.body.clientWidth:self.innerWidth-16;var m=!0;if(g>=
b||!a.sentFullPixel)m=!1;(0<=d||0<=c)&&(g>c&&e>d)&&(g="-"+(g-c),e="-"+(e-d));m&&"1"===a.heatmap&&(a.executePlugin(a.getPluginConfig("heatmap","before")),a.quicksend(a.wtEscape(a.contentId.split(";")[0])+","+g+","+e,"","hm"+(a.fileSuffix?".pl":"")),a.executePlugin(a.getPluginConfig("heatmap","after")))},c=function(){"undefined"!==typeof k.wt_heatmap?k.setTimeout(function(){k.wt_heatmap()},1E3):("undefined"===typeof k.wt_heatmap_retry&&(k.wt_heatmap_retry=0),k.wt_heatmap_retry++,60>k.wt_heatmap_retry&&
k.setTimeout(function(){c()},1E3))};a.heatmapTrackInit=function(){for(var b=!1,c=0;c<k.webtrekkHeatmapObjects.length;c++)a===k.webtrekkHeatmapObjects[c]&&(b=!0);!b&&(a.heatmap&&"1"===a.heatmap)&&(k.webtrekkHeatmapObjects.push(a),a.registerEvent(document,"mousedown",d),a.registerEvent(document,"touchstart",d))};a.heatmapTrackInit();a.heatmapOn&&!a.disableOverlayView&&a.startHeatmapOrOverlay("heatmap",c)},ja=function(a,d,c){a.setProperty("reporturl","report2.webtrekk.de/cgi-bin/wt");a.setProperty("disableOverlayView",
!1);var b=function(b){var c=a.reporturl;-1!==b.search(/^((http[s]?:\/\/)?(report\d+|analytics)\.webtrekk\.(com|de)\/)/)&&(c=b.split("/"),c.pop(),c=c.join("/"));return c};a.searchContentIds=function(){var b=0,c=0;a.contentIds=a.wtEscape(a.contentId.split(";")[0]);do{b++;var d=a.urlParam(document.location.href,"wt_contentId"+b,!1);d&&(a.contentIds+="&wt_contentId"+b+"="+a.wtEscape(d),c++)}while(c>=b)};a.startHeatmapOrOverlay=function(c,d){a.searchContentIds();a.urlParam(a.wtHref(),"wt_reporter",!1)?
a.reporturl=b(a.urlParam(a.wtHref(),"wt_reporter",!1)):a.getCookie("wt_overlayFrame")&&(a.reporturl=b(a.getCookie("wt_overlayFrame")));-1===a.reporturl.search(/http|https/)&&(a.reporturl=document.location.protocol+"//"+a.reporturl);if(a.contentIds){var g=a.reporturl+"/"+c+".pl?wt_contentId="+a.contentIds+"&x="+(new Date).getTime();if(a.include(g))if("heatmap"===c&&(-1!==navigator.userAgent.indexOf("MSIE 6")&&-1!==navigator.userAgent.indexOf("Windows NT 5.0"))&&alert("Click OK to start heatmap."),
"complete"!==document.readyState)a.registerEvent(k,"load",d);else return d()}return 0};(function(){for(var b=!1,h=0;h<k.webtrekkUnloadObjects.length;h++)a===k.webtrekkUnloadObjects[h]&&(b=!0);b||k.webtrekkUnloadObjects.push(a);ha(a);ia(a,d,c)})()},ka=function(){var a=this,d="customClickParameter customSessionParameter customCampaignParameter customEcommerceParameter customParameter crmCategory urmCategory contentGroup productCategory".split(" "),c={numberSearchResults:[d[4],771],errorMessages:[d[4],
772],loginStatus:[d[1],800],pixelVersion:[d[1],801],trackingPlatform:[d[1],802],npsScore:[d[6],850],npsScoreFeedback:[d[6],851],productLabel:[d[8],870],productMainCategory:[d[8],871],productSubCategory:[d[8],872],productManufacturer:[d[8],873],productShortDescription:[d[8],874],productLongDescription:[d[8],875],paymentMethod:[d[3],761],shippingService:[d[3],762],shippingSpeed:[d[3],763],shippingCosts:[d[3],764],grossMargin:[d[3],765],orderStatus:[d[3],766],productVariant:[d[3],767],couponValue:[d[3],
563],productSoldOut:[d[3],760],paywall:[d[4],773],articleTitle:[d[4],774],paywallUser:[d[6],852],pageMainCategory:[d[7],880],pageSubCategory:[d[7],881],pageAuthor:[d[7],882],contentTags:[d[4],775],pageTitle:[d[4],776],pageType:[d[4],777],pageLength:[d[4],778],daysSincePublication:[d[4],779],testVariant:[d[4],781],testExperiment:[d[4],782],email:[d[6],700],emailRID:[d[6],701],emailOptin:[d[6],702],firstName:[d[6],703],lastName:[d[6],704],telefon:[d[6],705],gender:[d[6],706],birthday:[d[6],707],country:[d[6],
708],city:[d[6],709],postalCode:[d[6],710],street:[d[6],711],streetNumber:[d[6],712],validation:[d[6],713]},b={},f=function(){for(var a=0;a<d.length;a++)b[d[a]]={}};f();this.list={};this.clear=function(){b={};a.list={};f()};this.get=function(c){c="all"===c?b:"undefined"!==typeof a.list[c]?a.list[c]:null;return c};this.put=function(d,f){if(f&&"undefined"!==typeof c[d]){var e=c[d];"undefined"===typeof b[e[0]]&&(b[e[0]]={});b[e[0]][e[1]]=f;a.list[d]=f}return a};this.remove=function(d){var f=null;if("undefined"!==
typeof c[d]){var e=c[d];"undefined"!==typeof b[e[0]]&&"undefined"!==typeof b[e[0]][e[1]]&&(f=b[e[0]][e[1]],delete b[e[0]][e[1]]);delete a.list[d]}return f}},la=function(a){a.PredefinedParameter=ka;a.checkCustomParameter=function(d,c){var b="";if("object"===typeof d)for(var f in d)isNaN(parseInt(f))||("undefined"===typeof d[f]||"string"!==typeof d[f]||""===d[f])||(a.checkSC("custom")&&(d[f]=a.decrypt(d[f])),-1===a.paramFirst.indexOf(c+f+";")&&(b+="&"+c+f+"="+a.wtEscape(d[f])));return b};a.mergeCustomParameter=
function(a,c){var b=a;b||(b={});for(var f in c)b[f]=c[f];return b}},ma=function(a,d,c){var b=this;b.item=c;b.href="undefined"!==typeof c.href?c.href:"";b.href||(b.href=a.getAttribute(c,"href")?a.getAttribute(c,"href"):"");b.linkIdByNameOrId=a.getAttribute(c,"name")?a.getAttribute(c,"name"):"";b.linkIdByNameOrId||(b.linkIdByNameOrId=a.getAttribute(c,"id")?a.getAttribute(c,"id"):"");b.linkId="";b.action="link";b.isDownloadFile=!1;b.linktrackOut=!1;b.isInternalLink=function(){var d;if(a.linkTrackDownloads){d=
a.linkTrackDownloads.split(";");for(var h=b.href.split("."),h=h.pop(),g=0;g<d.length;g++)if(d[g]===h){b.isDownloadFile=!0;break}}b.linktrackOut=a.domain&&!a.isOwnDomain(b.href);if(b.isDownloadFile||"_blank"===c.target)b.action="click";d=b.href;var h=d.toLowerCase(),g=d.split("#")[0],e=document.location,k=b.item,p=a.getAttribute,l=p(k,"onclick"),n=p(k,"onmousedown"),k=p(k,"ontouchstart");d=a.noDelayLinkTrackAttribute?!!a.getAttribute(c,a.noDelayLinkTrackAttribute):!(d&&!(0===h.indexOf("javascript:")||
0===h.indexOf("#")||"click"===b.action||g===e.href.split("#")[0]&&-1!==d.indexOf("#")||g===e.pathname.split("#")[0]&&-1!==d.indexOf("#")||l&&-1!==l.search(/return false[;]?$/)||n&&-1!==n.search(/return false[;]?$/)||k&&-1!==k.search(/return false[;]?$/)));return d};b.getCCParams=function(){var c="";if(a.config.customClickParameter){var d="undefined"!==typeof a.config.customClickParameter[b.linkIdByNameOrId]?a.config.customClickParameter[b.linkIdByNameOrId]:!1;d||(d=a.config.customClickParameter);
for(var g in d)!isNaN(parseInt(g))&&("string"===typeof d[g]&&d[g])&&(a.checkSC("custom")&&(d[g]=a.decrypt(d[g])),c+="&ck"+g+"="+a.wtEscape(d[g]))}return c};b.setJSONParams=function(){b.linkId||(b.linkId=a.getAttribute(c,a.linkTrackAttribute));null!==a.getJSON(b.linkId)&&(a.parseJSON(a.getJSON(b.linkId)),b.linkId=a.config.linkId)};b.getLinkId=function(){b.linkId=a.getAttribute(c,a.linkTrackAttribute);b.setJSONParams();if("link"===a.linkTrack){var d=b.href.indexOf("//");b.href=0<=d?b.href.substr(d+
2):b.href;a.linkTrackPattern&&(a.linkTrackReplace||(a.linkTrackReplace=""),b.href=b.href.replace(a.linkTrackPattern,a.linkTrackReplace));b.linkId=(b.linkId?b.linkId+".":"")+b.href.split("?")[0].split("#")[0].replace(/\//g,".");d=[];a.linkTrackParams&&(d=a.linkTrackParams.replace(/;/g,",").split(","));for(var h=0;h<d.length;h++){var g=a.urlParam(b.href,d[h],"");g&&(b.linkId+="."+d[h]+"."+g)}}return b.linkId}},na=function(a){var d=this;d.triggerObjectName="__"+(new Date).getTime()+"_"+parseInt(1E3*
Math.random());var c=function(b,c){var g=c[d.triggerObjectName];a.config=a.getConfig(!0);a.config.customClickParameter=a.customClickParameter;a.ccParams=g.getCCParams();var e=a.config.linkId=g.getLinkId();a.linktrackOut=g.linktrackOut;a.sendinfo(a.config,e,g.action)},b=function(b){a.registerEvent(b,"click",function(h){if(h.which&&1===h.which||h.button&&1===h.button)a.delayLinkTrack&&("function"===typeof h.preventDefault&&!b[d.triggerObjectName].isInternalLink())&&(h.preventDefault(),k.setTimeout(function(){document.location.href=
b.href},a.delayLinkTrackTime)),c(h,b)})};d.linkTrackInit=function(){if(a.linkTrack&&("link"===a.linkTrack||"standard"===a.linkTrack)){for(var c=!1,h=0;h<k.webtrekkLinktrackObjects.length;h++)a===k.webtrekkLinktrackObjects[h]&&(c=!0);c||k.webtrekkLinktrackObjects.push(a);c=0;for(h=document.links.length;c<h;c++){var g=document.links[c],e=a.getAttribute(g,a.linkTrackAttribute),m=a.getAttribute(g,"href");(a.linkTrackIgnorePattern&&m&&-1===m.search(a.linkTrackIgnorePattern)||!a.linkTrackIgnorePattern)&&
("undefined"===typeof g[d.triggerObjectName]&&(e||"link"===a.linkTrack))&&(g[d.triggerObjectName]=new ma(a,d,g),b(g))}}}},oa=function(a){a.linkTrackObject=new na(a);a.linkTrackInstall=a.linkTrackObject.linkTrackInit;a.linkTrackInit=a.linkTrackObject.linkTrackInit;"complete"===document.readyState?a.linkTrackInit():a.registerEvent(k,"load",function(){a.linkTrackInit()})},pa=function(a,d,c){var b=null,f=d.type,h=a.getFormFieldName(d),g=a.getFormFieldValue(d);this.close=function(){null!==b&&(g=a.getFormFieldValue(d),
"radio"!==f&&(k.clearInterval(b),b=null,a.formFieldData[h][c]=[f,g]),a.formFieldDataPathAnalysis.push([h,f,g]))};this.start=function(e){null===b&&(b=k.setInterval(function(){"undefined"!==typeof d&&(d&&null!==d&&"undefined"!==typeof a.formFieldData[h])&&("string"!==typeof a.formFieldData[h][0]&&"string"!==typeof a.formFieldData[h][a.formFieldData.length-1])&&(g=a.getFormFieldValue(d),a.formFieldData[h][c]=[f,g])},50),e||delete a.formFieldDataUnused[h])}},C=function(a,d){var c=this;c.formObject=!1;
c.formFocus=!1;c.formName=!1;c.form=a.form;c.formSubmit=!1;c.formFieldData={};c.formFieldDataUnused={};c.formFieldDataPathAnalysis=[];c.triggerObjectName="__"+(new Date).getTime()+"_"+parseInt(1E3*Math.random());var b=function(a){return"hidden"!==a&&"button"!==a&&"image"!==a&&"reset"!==a&&"submit"!==a&&"fieldset"!==a},f=function(a){return"select-multiple"!==a&&"select-one"!==a&&"checkbox"!==a&&"radio"!==a},h=function(a,d,e){b(a.type)&&c.formObject&&(d||(c.formFocus=a),"undefined"===typeof a[c.triggerObjectName]&&
(a[c.triggerObjectName]=new pa(c,a,e),a[c.triggerObjectName].start(d)))},g=function(d,e){a.registerEvent(d,"focus",function(){h(d,!1,e)});a.registerEvent(d,"blur",function(){b(d.type)&&(c.formObject&&d&&d&&"undefined"!==typeof d[c.triggerObjectName])&&(d[c.triggerObjectName].close(),"radio"!==d.type&&delete d[c.triggerObjectName])})},e=function(){if(c.form&&!c.formObject)for(var a=document.forms,b=0,d=a.length;b<d;b++){var e=a[b];if("undefined"!==typeof e.elements.wt_form){c.formObject=e;break}}},
m=function(a){!c.form||a.target!==c.formObject&&a.srcElement!==c.formObject||(c.formSubmit=!0)},p=function(){e();if(c.formObject){var f=a.getAttribute(c.formObject,a.formAttribute);c.formName=f?f:a.contentId.split(";")[0];for(var f=0,v=c.formObject.elements,da=v.length;f<da;f++){var w=v[f],l=c.getFormFieldName(w);if(b(w.type)&&l&&null!==l){"undefined"===typeof c.formFieldData[l]&&(c.formFieldData[l]=[]);"undefined"===typeof c.formFieldDataUnused[l]&&(c.formFieldDataUnused[l]=[]);var n=c.getFormFieldValue(w);
c.formFieldData[l].push([w.type,n]);c.formFieldDataUnused[l].push([w.type,n]);l=c.formFieldData[l].length-1;"radio"===w.type&&w.checked&&h(w,!0,l);g(w,l)}}a.registerEvent(c.formObject,"submit",m);a.registerEvent(k,"beforeunload",function(){d(c)});a.registerEvent(k,"unload",function(){d(c)})}},l=function(b){var d=[];a.formFullContent&&(d=a.formFullContent.split(";"));if(a.formAnonymous||f(b.type)){for(var e=0;e<d.length;e++)if(d[e]===c.getFormFieldName(b))return!1;return!0}return!1},n=function(b,c){var d=
c;d||(d=b);var e=a.getAttribute(d,a.formValueAttribute).replace(/[\.;\|]/g,"_");return f(b.type)?a.maxlen(a.wtUnescape(d.value),110):l(b)?"anon":a.maxlen(a.wtUnescape(e),110)},r=function(a,b,d,e){var f=a.replace(/[\.;\|]/g,"_")+".",f=f+(b+"|")+(d+"|");return f=e?f+"0":f+(c.formFocus&&c.getFormFieldName(c.formFocus)===a?"1":"0")},q=function(a,b){for(var c=[],d=0,e=b.length;d<e;d++)if("undefined"!==typeof a[b[d]])if("select-multiple"===a[b[d]][0])for(var f=a[b[d]][1].split("|"),h=0,g=f.length;h<g;h++)c.push(r(b[d],
a[b[d]][0],f[h]));else c.push(r(b[d],a[b[d]][0],a[b[d]][1]));return c.join(";")},s=function(a){for(var b in a){var c=b+"";if(1<a[c].length){for(var d=!1,e=0;e<a[c].length;e++)if("empty"!==a[c][e][1]){a[c]=a[c][e];d=!0;break}if(d)continue}a[c]=a[c][0]}},u=function(){if(!c.formObject)return"";s(c.formFieldData);s(c.formFieldDataUnused);var b=[],d;d=[];if("undefined"!==typeof c.formObject.elements.wt_fields){var e=c.formObject.elements.wt_fields.value;e&&(d=e.split(";"))}if(0>=d.length)for(var f in c.formFieldData)"string"===
typeof f&&f&&d.push(f);e=!1;if(a.formPathAnalysis){(f=q(c.formFieldDataUnused,d))&&b.push(f);f=0;for(var h=c.formFieldDataPathAnalysis.length;f<h;f++){var g=c.formFieldDataPathAnalysis,k;a:{k=0;for(var l=d.length;k<l;k++)if(g[f][0]===d[k]){k=!0;break a}k=!1}k&&(e=!0,b.push(r(g[f][0],g[f][1],g[f][2],!0)))}e&&(d=b[b.length-1],d=d.substr(0,d.length-1),b[b.length-1]=d+"1")}else return q(c.formFieldData,d);return b.join(";")};c.getFormFieldName=function(b){var c=b.name;a.formFieldAttribute&&(c="",(b=a.getAttribute(b,
a.formFieldAttribute))||null!==b)&&(c=b);return c};c.getFormFieldValue=function(b){var d=b.type,e="";if("select-multiple"===d){for(var e=[],f=0,h=b.options,g=h.length;f<g;f++)h[f].selected&&e.push(n(b,h[f]));0>=e.length&&e.push("empty");e=e.join("|")}else"select-one"===d?(e="",-1!==b.selectedIndex&&((e=n(b,b.options[b.selectedIndex]))&&null!==e||(e="empty"))):"checkbox"===d||"radio"===d?(e="",b.checked?(e=n(b))||(e="checked"):e="empty"):"hidden"!==d&&("button"!==d&&"image"!==d&&"reset"!==d&&"submit"!==
d)&&(f=(e=n(b))?"filled_out":"empty",l(b)||(f=e),h=c.getFormFieldName(b),"undefined"!==typeof a.formFieldDefaultValue[h]&&a.formFieldDefaultValue[h]===e&&"empty"!==f&&(f="empty"),f&&null!==f||(f="empty"),e=f);return l(b)&&"select-multiple"!==d&&"empty"!==e&&"filled_out"!==e?"anon":e};c.formTrackInstall=function(a){c.formObject=a?a:document.forms[0]?document.forms[0]:!1;c.formObject&&(c.form="1",p())};c.getFormTrackingData=function(){var b="";if(c.formObject){var d=u();if(d){var e=c.formSubmit;a.formSubmit&&
(e=a.formSubmit);b+="&fn="+a.wtEscape(c.formName)+"%7C"+(e?"1":"0");b+="&ft="+a.wtEscape(d)}c.formSubmit=!1;a.formSubmit=!1;c.formName=!1;c.formObject=!1;c.formFocus=!1;c.formFieldData={};c.formFieldDataUnused={};c.formFieldDataPathAnalysis=[]}return b};c.formTrackSubmit=function(){c.formSubmit=!0};c.sendFormRequest=function(){"1"!==a.cookie||(a.optOut||a.deactivatePixel)||a.firstParty();"function"===typeof a.beforeUnloadPixel?a.beforeUnloadPixel():a.executePlugin(a.getPluginConfig("form","before"));
var b=c.getFormTrackingData();b&&a.quicksend(a.wtEscape(a.contentId.split(";")[0])+",1,"+a.baseparams(),b);"function"===typeof a.afterUnloadPixel?a.afterUnloadPixel():a.executePlugin(a.getPluginConfig("form","after"))};c.updateFormFieldStatus=function(a){if(a&&null!==a&&"undefined"!==typeof a.type){var b=a.type,d=c.getFormFieldName(a),e=c.getFormFieldValue(a);d&&(e&&"undefined"!==typeof c.formFieldData[d])&&(c.formFieldData[d].splice(0,0,[b,e]),c.formFieldDataPathAnalysis.push([d,b,e]),c.formFocus=
a,delete c.formFieldDataUnused[d])}};c.init=function(){c.form&&("1"===c.form&&!c.formObject)&&p();return c}},qa=function(a){a.multipleFormArray=[];var d=function(b){for(var c=0;c<a.multipleFormArray.length;c++)if(a.multipleFormArray[c].formObject===b)return a.multipleFormArray[c];return!1},c=function(b){if(b&&"undefined"!==typeof b.elements&&!d(b)){var c=new C(a,function(b){z(a,"form","form",b)});a.multipleFormArray.push(c);c.formTrackInstall(b)}};a.multipleFormTrackInstall=c;a.multipleFormTrackInit=
c;a.multipleFormTrackSubmit=function(a){a&&(a=d(a))&&a.formTrackSubmit()};a.sendMultipleFormRequest=function(b){if(b)(b=d(b))&&b.sendFormRequest();else for(b=0;b<a.multipleFormArray.length;b++)a.multipleFormArray[b].sendFormRequest()};a.updateMultipleFormFieldStatus=function(a,c){if(a){var h=d(a);h&&h.updateFormFieldStatus(c)}}},ra=function(a,d){var c=this;c.name=a?a:"";c.elements=d?d:[];c.add=function(a){a&&null!==a&&c.elements.push(a);return c};c.getAttribute=function(){return a};c.addEventListener=
c.removeEventListener=function(){}},z=function(a,d,c,b){"1"!==a.cookie||(a.optOut||a.deactivatePixel)||a.firstParty();(c=c?c:"")||(c=a.formObject&&"noForm"!==d?"form":"link");"function"===typeof a.beforeUnloadPixel?a.beforeUnloadPixel():"form"===c&&a.executePlugin(a.getPluginConfig("form","before"));var f="";if(a.config.linkId&&(f+="&ct="+a.wtEscape(a.maxlen(a.wtUnescape(a.config.linkId),255)))){a.linktrackOut&&(f+="&ctx=1");var h=a.ccParams;"string"===typeof h&&""!==h&&(f+=h)}if(a.wtEp)if(a.wtEpEncoded)f+=
a.wtEp;else if(h=a.wtEp,"string"===typeof h&&""!==h)for(var h=h.split(/;/),g=0;g<h.length;g++)if("undefined"!==typeof h[g]){var e=h[g].split(/\=/);a.checkSC("custom")&&(e[1]=a.decrypt(e[1]));e[1]=a.wtEscape(e[1]);f+="&"+e[0]+"="+e[1]}"noForm"!==d&&(f+=b.getFormTrackingData());""!==f&&(a.quicksend(a.wtEscape(a.contentId.split(";")[0])+",1,"+a.baseparams(),f),a.config.linkId="",a.ccParams="",a.wtEp="");"function"===typeof a.afterUnloadPixel?a.afterUnloadPixel():"form"===c&&a.executePlugin(a.getPluginConfig("form",
"after"))},sa=function(a){a.formTrackObject=new C(a,function(d){z(a,"form","form",d)});a.formTrackInstall=a.formTrackObject.formTrackInstall;a.formTrackInit=a.formTrackObject.formTrackInstall;a.formTrackSubmit=a.formTrackObject.formTrackSubmit;a.sendFormRequest=a.formTrackObject.sendFormRequest;a.updateFormFieldStatus=a.formTrackObject.updateFormFieldStatus;"complete"===document.readyState?a.formTrackObject.init():a.registerEvent(k,"load",function(){a.formTrackObject.init()});a.CustomForm=ra;qa(a)},
ta=function(){var a="",d="",c=!1,b=!0,f=0,h=null,g=!1,e=function(){},m=function(){},p=function(a,b,c){a.addEventListener?a.addEventListener(b,c,!1):a.attachEvent&&a.attachEvent("on"+b,c)},l=function(){var a=1E3;h=k.setInterval(function(){0>=a&&(k.clearInterval(h),g=!1,c||(b=!0,f++,e(f)));a-=5},5)};this.isPageHidden=function(){return c};this.isPageVisible=function(){return b};this.setVisibleCall=function(a){e=a};this.setHiddenCall=function(a){m=a};"undefined"!==typeof document.hidden?(a="hidden",d=
"visibilitychange"):"undefined"!==typeof document.mozHidden?(a="mozHidden",d="mozvisibilitychange"):"undefined"!==typeof document.msHidden?(a="msHidden",d="msvisibilitychange"):"undefined"!==typeof document.oHidden?(a="oHidden",d="ovisibilitychange"):"undefined"!==typeof document.webkitHidden&&(a="webkitHidden",d="webkitvisibilitychange");a&&(c=document[a],(b=!c)&&f++,function(){p(document,d,function(){(c=document[a])?(k.clearInterval(h),b=g=!1,m()):g||(g=!0,l())})}())},ua=function(a){a.setProperty("tabBrowsing",
!1);var d=[],c=[],b=null,f=function(){b=new ta;b.setVisibleCall(function(b){if(1<b&&0<c.length){var f=c[0].split("&");b-=1;f[1]="tb="+b+"&cp770="+b+"&"+f[1];a.sendPixel(f.join("&"),c[1])}for(var e=0,k=d.length;e<k;e++)f=d[e][0],b=d[e][1],a.sendPixel(f,b),"wt"===b&&-1===f.search(/&(ct|fn|ft)=.+&/)&&(c=[f,b]);d=[]})};a.tabBrowsing&&f();a.sendTabBrowsingPixel=function(f,g){b.isPageVisible()?(a.sendPixel(f,g),"wt"===g&&-1===f.search(/&(ct|fn|ft)=.+&/)&&(c=[f,g])):d.push([f,g])};a.startTabBrowsing=function(){null===
b&&(a.tabBrowsing=!0,f())}};k.webtrekkV3=k.WebtrekkV3=function(a){var d=k.webtrekkConfig;a||(a=k.webtrekkConfig);this.version="441";for(var c=[L,W,X,$,ba,ca,ga,ja,la,oa,sa,ua],b=0;b<c.length;b++)c[b](this,a,d)}})(window);

if("object"===typeof webtrekkConfig&&"object"===typeof webtrekkConfig.safetag&&-1===document.cookie.indexOf("wt_r=1")){var wts=wts||[],wt_safetagConfig={async:"undefined"!==typeof webtrekkConfig.safetag.async?webtrekkConfig.safetag.async:!0,timeout:"undefined"!==typeof webtrekkConfig.safetag.timeout?webtrekkConfig.safetag.timeout:2E3,safetagDomain:"undefined"!==typeof webtrekkConfig.safetag.safetagDomain?webtrekkConfig.safetag.safetagDomain:!1,safetagId:"undefined"!==typeof webtrekkConfig.safetag.safetagId?
webtrekkConfig.safetag.safetagId:!1,customDomain:"undefined"!==typeof webtrekkConfig.safetag.customDomain?webtrekkConfig.safetag.customDomain:!1,customPath:"undefined"!==typeof webtrekkConfig.safetag.customPath?webtrekkConfig.safetag.customPath:!1,option:"undefined"!==typeof webtrekkConfig.safetag.option?webtrekkConfig.safetag.option:{}};(function(b,g){var d=function(a){try{return encodeURIComponent(a)}catch(b){return escape(a)}},e=document.location.protocol;if("http:"==e||"https:"==e){var a="";b.customDomain&&
b.customPath?a=e+"//"+b.customDomain+"/"+b.customPath:b.safetagDomain&&(b.safetagId&&-1!==b.safetagId.search(/^\d+$/))&&(a=e+"//"+b.safetagDomain+"/resp/api/get/"+b.safetagId+"?url="+d(document.location.href));if(a){for(var f in b.option)a+="&"+f+"="+d(b.option[f]);webtrekkV3.prototype.safetag=b;webtrekkV3.prototype.safetagInProgress=!0;webtrekkV3.prototype.safetagParameter={};webtrekkV3.prototype.update=function(){};window.wts=g;window.safetagLoaderHandler=function(a,b){if(a&&b)if("onerror"==b)webtrekkV3.prototype.safetagInProgress=
!1;else if("onload"==b){if("undefined"!==typeof wt_r&&!isNaN(wt_r)){var d=new Date;document.cookie="wt_r=1;path=/;expires="+d.toUTCString(d.setTime(d.getTime()+1E3*parseInt(wt_r)))}webtrekkV3.prototype.safetagInProgress=!1}else"onreadystatechange"!=b||"loaded"!=a.readyState&&"complete"!=a.readyState||(a.onreadystatechange=null,a.onload(c))};if(b.async||"complete"===document.readyState||"interactive"===document.readyState){var d=document.getElementsByTagName("script")[0],c=document.createElement("script");
c.async=b.async;c.type="text/javascript";c.onerror=function(){"undefined"!==typeof window.wt_mcp_hide&&"function"===typeof window.wt_mcp_hide.show&&(window.wt_mcp_hide.show(),window.wt_mcp_hide.show=function(){});safetagLoaderHandler(c,"onerror")};c.onload=function(){safetagLoaderHandler(c,"onload")};c.onreadystatechange=function(){safetagLoaderHandler(c,"onreadystatechange")};c.src=a;d.parentNode.insertBefore(c,d)}else a='<script type="text/javascript" src="'+a+'" onerror="safetagLoaderHandler(this,\'onerror\')"',
a+=" onload=\"safetagLoaderHandler(this,'onload')\"",a+=" onreadystatechange=\"safetagLoaderHandler(this,'onreadystatechange')\"",a+=">\x3c/script>",document.write(a)}}})(wt_safetagConfig,wts)};

/** End webtrekk_v4.js */
/** javascripts/changeSite.js **/
$(document).ready(function() {
    'use strict';

    $('#close-change-site-message').on('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        $('#change-site-message').hide();
        $.cookie('changeSiteMessageHidden', 'true', {path: "/"});
    });
});