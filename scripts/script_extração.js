var painels = document.getElementsByClassName('fusion-panel panel-default');

txt = "Novela;Ano\n"

for (let i = 0;i<painels.length;i++){
    listaNovelas = painels[i].getElementsByTagName('a')

    ano =  listaNovelas[0].innerText

    for(let j = 1;j<listaNovelas.length;j++){
        novela =  listaNovelas[j].innerText
        txt+=novela+";"+ano+"\n"
    }

}

var element = document.createElement('a');
		element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(txt));
		element.setAttribute('download', 'novelas.csv');

		element.style.display = 'none';
		document.body.appendChild(element);

		element.click();

		document.body.removeChild(element);