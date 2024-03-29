/*{ Добавление новых маркеров «Полигон», «Горячие точки», ... для карт */
var maps = window.NetDB.namespace('maps');

maps.registerMarkerType({
    slug: 'beauty',
    name: 'Красивый маркер',
    showLegend: true,
    size: 60,
    icon: function (cluster, features, legend) {
        let data = features.collectProperties(cluster.getAllChildMarkers()),
            customColor,
            sum,
            colors = features._initColors();
        _.each(data, prop => {
            if (prop.value) {
                customColor = prop.color || colors[prop.name];
                sum = (sum || 0) + prop.value;
            }
        });

        var iconSettings = {
            mapIconUrl: '<svg xmlns="http://www.w3.org/2000/svg" transform="translate(-20,-40)" width="48" height="48" viewBox="0 0 48 48"' +
                        ' style="width: 48px; height: 48px; transform: translate(-20px,-40px);" >' +
                        '<g transform="scale(0.6)"><ellipse cx="41" cy="75" rx="10" ry="5" fill="grey" style="cursor: pointer;" opacity="0.6"></ellipse>' +
                        '<path fill="{mapIconColor}" d="M38.9988 73.2875L40.7863 75.2375L42.7363 73.2875C43.7113 71.9875 67.5988 44.85 67.5988 30.7125C67.5988' +
                        '15.925 55.5738 4.0625 40.9488 4.0625C26.1613 4.0625 14.2988 16.0875 14.2988 30.7125C14.2988 44.85 38.1863 72.15 38.9988 73.2875Z" style=""></path>' +
                        '<circle cx="41" cy="31" r="19" fill="white"></circle>' +
                        '</g></svg>',
            mapIconColor: customColor ? customColor : '#2c84cb',
            mapIconTitle: sum
        };

        var defaultIcon = L.divIcon({
            className: "map-default-marker",
            html: sum ? L.Util.template(iconSettings.mapIconUrl, iconSettings) : null,
        });
        return defaultIcon;
    }
});

maps.registerMarkerType({
    slug: 'ground',
    name: 'Полигон',
    size: 60,
    icon: function (cluster, features, legend) {
        let data = features.collectProperties(cluster.getAllChildMarkers()),
            customColor,
            sum,
            colors = features._initColors();
        _.each(data, prop => {
            if (prop.value) {
                customColor = prop.color || colors[prop.name];
                sum = (sum || 0) + prop.value;
            }
        });

        var iconSettings = {
            mapIconUrl: '<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" transform="translate(-20,-40)" ' +
                        'version="1.1" id="Capa_1" x="0px" y="0px" width="60" height="60" viewBox="0 0 70 85" ' +
                        'style="width: 60px; height: 60px; transform: translate(-20px,-40px);" ' +
                        'enable-background="new 0 0 628.254 613.516" xml:space="preserve" fill="{mapIconColor}">' +
                        '<ellipse cx="25" cy="56" rx="8" ry="4" fill="grey" fill-opacity="undefined" stroke="none" cursor="pointer" transform="translate(-1,-8)scale(1.5)"></ellipse>' +
                        '<polygon points="21,44 29,44 25,56" style="fill: white;" fill="white" stroke="none" cursor="pointer" transform="translate(-1,-8)scale(1.5)"/>' +
                        '<g transform="scale(0.11)">' +
                        '<circle cx="320" cy="300" r="250" fill="white" cursor="pointer"/>' +
                        '<path stroke="black" d="M99.777,446.086c-6.699,12.031-12.031,30.133-12.031,41.539c0,2.648,0,6.016,0.656,10.688L4.676,354.32 ' +
                        'C1.996,349.648,0,342.945,0,336.906c0-6.047,1.996-13.398,4.676-18.078L44.871,248.5L0,223.07l146.016-2.703L217,348.281 ' +
                        'l-45.527-26.117L99.777,446.086z M164.09,40.227c12.715-22.125,33.496-34.18,58.926-34.18c27.48,0,48.918,12.742,64.312,38.828 ' +
                        'l22.777,38.172l-79.051,136.664l-127.914-74.352L164.09,40.227z M173.441,561.945c-38.172,0-69.645-31.477-69.645-69.648 ' +
                        'c0-10.719,4.703-28.82,11.402-40.195l21.41-38.172h158.758v148.016H173.441z M300.727,36.18 ' +
                        'C290.691,18.789,277.293,6.703,261.215,0h164.746c14.738,0,26.113,6.047,32.84,17.445l40.852,69.648l44.191-26.141 ' +
                        'l-71.016,127.281l-145.305-2.047l44.871-25.43L300.727,36.18z M554.59,415.273c20.07,0,36.832-5.359,50.887-16.055 ' +
                        'l-83.07,144.648c-6.699,11.375-18.73,18.078-32.789,18.078h-78.395v51.57l-75.004-125.234l75.004-125.266v52.258H554.59z ' +
                        'M618.848,294.711c6.043,10.719,9.406,22.094,9.406,34.156c0,24.117-15.422,49.57-36.832,61.602 ' +
                        'c-10.062,5.391-24.145,8.75-38.172,8.75h-44.242l-79-136.664l127.918-73.008L618.848,294.711z"/>' +
                        '</g>' +
                        '</svg><span>{mapIconTitle}</text>',
            mapIconColor: customColor ? customColor : '#2c84cb',
            mapIconTitle: sum
        };

        var defaultIcon = L.divIcon({
            className: "map-ground-marker",
            html: L.Util.template(iconSettings.mapIconUrl, iconSettings),
        });
        return defaultIcon;
    }
});

maps.registerMarkerType({
    slug: 'hotpoint',
    name: 'Горячие точки',
    size: 60,
    icon: function () {
        var iconSettings = {
            mapIconUrl: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 40 60" width="60" height="60" transform="translate(-20,-40)" ' +
                        'style="width: 60px; height: 60px; transform: translate(-20px,-40px);">' +
                        '<ellipse cx="25" cy="56" rx="8" ry="4" fill="grey" fill-opacity="undefined" stroke="none" cursor="pointer" transform="translate(-10,-18)scale(1.2)"></ellipse>' +
                        '<polygon points="21,44 29,44 25,56" style="fill: white;" fill="white" stroke="none" cursor="pointer" transform="translate(-15,-28)scale(1.4)"/>' +
                        '<circle cx="20" cy="22" r="10" fill="white" cursor="pointer"/>' +
                        '<path d="M569.517 440.013C587.975 472.007 564.806 512 527.94 512H48.054c-36.937 0-59.999-40.055-41.577-71.987L246.423 ' +
                        '23.985c18.467-32.009 64.72-31.951 83.154 0l239.94 416.028zM288 354c-25.405 0-46 20.595-46 46s20.595 46 46 46 46-20.595 ' +
                        '46-46-20.595-46-46-46zm-43.673-165.346l7.418 136c.347 6.364 5.609 11.346 11.982 11.346h48.546c6.373 0 11.635-4.982 ' +
                        '11.982-11.346l7.418-136c.375-6.874-5.098-12.654-11.982-12.654h-63.383c-6.884 0-12.356 5.78-11.981 12.654z" transform="scale(0.07)" fill="#cd5c5c" stroke="black"/>' +
                        '</svg>'
        };
        return L.divIcon({
            html: L.Util.template(iconSettings.mapIconUrl),
            iconSize: 0,
            className: 'map-hotpoint-marker'
        });
    }
});

maps.registerMarkerType({
    slug: 'circle_settings',                                            // *settings* включает меню настрйки цвета/пиктограммы
    name: 'Круг',
    showLegend: true,
    size: 60,
    icon: function (cluster, features, legend) {
        let data = features.collectProperties(cluster.getAllChildMarkers()),
            customColor,
            sum,
            customPictogram,
            colors = features._initColors();
        _.each(data, prop => {
            if (prop.value) {
                customColor = prop.color || colors[prop.name];
                sum = (sum || 0) + prop.value;
            }
        });
        try {
            if (gv_maps.markers.colors != undefined) {
                gv_maps.markers.colors.forEach(function(elem, index, mass) {                    // Цвет маркета согласно настройкам
                    if ( elem.key == data[0].formattedValue) {customColor = elem.val}               // из блока settings
                    if ( elem.key == data[0].key[1]) {customColor = elem.val}                       // из блока settings
                });
            }
            if (gv_maps.markers.icons != undefined) {
                gv_maps.markers.icons.forEach(function(elem, index, mass) {                     // Пиктограмма маркета согласно настройкам
                    if ( elem.key == data[0].formattedValue) {customPictogram = elem.val}           // из блока settings
                    if ( elem.key == data[0].key[1]) {customPictogram = elem.val}                   // из блока settings
                });
            }
        } catch (e) {}

        var iconSettings = {
            mapIconUrl: '<svg transform="translate(-30,-30)" width="55" height="48" '+              // Подгоняем маркер к координатам
                        'style="width: 55px; height: 48px; transform: translate(-30px,-30px);"> ' + // Для планшета (атрибут не поддерживается)
                        '<g transform="scale(0.7)">' +
                        '<circle cx="41" cy="31" r="19" fill="{mapIconColor}"></circle>' +
                        '</g>'+
                        '</svg>' +
                        '<i class="{mapIconPictogram}"' +
                        'style="top: -36.5px; left: -25.5px; font-size: 16px; color: white; transform: scale(0.75);"' +       // Подгоняем пиктограмму в центр иконки
                        '></i>',
            mapIconColor: customColor ? customColor : '#2c84cb',
            mapIconTitle: sum,
            mapIconPictogram: customPictogram ? customPictogram : ""
        };

        var defaultIcon = L.divIcon({
            className: "map-default-marker",
            html: sum ? L.Util.template(iconSettings.mapIconUrl, iconSettings) : null,
        });
        return defaultIcon;
    }
});

maps.registerMarkerType({
    slug: 'simple_incl_wh_settings',                                            // *settings* включает меню настрйки цвета/пиктограммы
    name: 'Простой с наклоном (белый в центре)',
    showLegend: true,
    size: 60,
    icon: function (cluster, features, legend) {
        let data = features.collectProperties(cluster.getAllChildMarkers()),
            customColor,
            customPictogram,
            sum,
            colors = features._initColors();
        _.each(data, prop => {
            if (prop.value) {
            customColor = prop.color || colors[prop.name];
            sum = (sum || 0) + prop.value;
            }
        });
        try {
            if (gv_maps.markers.colors != undefined) {
                gv_maps.markers.colors.forEach(function(elem, index, mass) {                    // Цвет маркета согласно настройкам
                    if ( elem.key == data[0].formattedValue) {customColor = elem.val}               // из блока settings
                    if ( elem.key == data[0].key[1]) {customColor = elem.val}                       // из блока settings
                });
            }
            if (gv_maps.markers.icons != undefined) {
                gv_maps.markers.icons.forEach(function(elem, index, mass) {                     // Пиктограмма маркета согласно настройкам
                    if ( elem.key == data[0].formattedValue) {customPictogram = elem.val}           // из блока settings
                    if ( elem.key == data[0].key[1]) {customPictogram = elem.val}                   // из блока settings
                });
            }
        } catch (e) {}

        var iconSettings = {
            mapIconUrl: '<svg transform="translate(-30,-60)" width="58" height="71" ' +             // Подгоняем маркер к координатам
                        'style="width: 58px; height: 71px; transform: translate(-30px,-60px);"> ' + // Для планшета (атрибут не поддерживается)
                        '<g transform="scale(0.7)">' +
                        '<ellipse cx="51" cy="96" rx="10" ry="5" fill="grey" style="cursor: pointer;" opacity="0.6"></ellipse>' +
                        '<path fill="{mapIconColor}" d="M 59.593937 0.4826568 A 24.067799 23.533833 47.23309 0 0 47.348356 31.57908 ' +
                        'A 24.067799 23.533833 47.23309 0 0 78.671954 44.544372 ' +
                        'A 24.067799 23.533833 47.23309 0 0 79.887013 43.996453 ' +
                        'A 19.568363 1.1313482 82.519857 0 0 80.878789 52.415658 ' +
                        'A 19.568363 1.1313482 82.519857 0 0 83.975043 70.439899 ' +
                        'A 2.5175273 25.119949 9.0413241 0 0 84.542804 71.789906 ' +
                        'A 2.5175273 25.119949 9.0413241 0 0 90.973935 47.425829 ' +
                        'A 2.5175273 25.119949 9.0413241 0 0 92.913413 22.971299 '+
                        'A 24.067799 23.533833 47.23309 0 0 90.917742 13.448427 ' +
                        'A 24.067799 23.533833 47.23309 0 0 59.593937 0.4826568"' +
                        'transform="matrix(0.92443322,0.38134397,-0.40026478,0.91639953,0,0)"/>' +
                        '<circle cx="55" cy="47" r="16" fill="white"></circle>' +
                        '</g>' +
                        '</svg>' +
                        '<i class="{mapIconPictogram}" ' +
                        'style="top: -51.5px; left: -16px; font-size: 16px; color: black;"' +       // Подгоняем пиктограмму в центр иконки
                        '></i>',
            mapIconColor: customColor ? customColor : '#2c84cb',
            mapIconTitle: sum,
            mapIconPictogram: customPictogram ? customPictogram : ""
        };

        var defaultIcon = L.divIcon({
            className: "map-default-marker",
            html: sum ? L.Util.template(iconSettings.mapIconUrl, iconSettings) : null,
        });
        return defaultIcon;
    }
});

maps.registerMarkerType({
    slug: 'simple_incl_settings',                                               // *settings* включает меню настрйки цвета/пиктограммы
    name: 'Простой с наклоном',
    showLegend: true,
    size: 60,
    icon: function (cluster, features, legend) {
        let data = features.collectProperties(cluster.getAllChildMarkers()),
            customColor,
            customPictogram,
            sum,
            colors = features._initColors();
        _.each(data, prop => {
            if (prop.value) {
            customColor = prop.color || colors[prop.name];
            sum = (sum || 0) + prop.value;
            }
        });
        customColor = "#2c84cb";
        try {
            if (gv_maps.markers.colors != undefined) {
                gv_maps.markers.colors.forEach(function(elem, index, mass) {                    // Цвет маркета согласно настройкам
                    if ( elem.key == data[0].formattedValue) {customColor = elem.val}               // из блока settings
                    if ( elem.key == data[0].key[1]) {customColor = elem.val}                       // из блока settings
                });
            }
            if (gv_maps.markers.icons != undefined) {
                gv_maps.markers.icons.forEach(function(elem, index, mass) {                     // Пиктограмма маркета согласно настройкам
                    if ( elem.key == data[0].formattedValue) {customPictogram = elem.val}           // из блока settings
                    if ( elem.key == data[0].key[1]) {customPictogram = elem.val}                   // из блока settings
                });
            }
        } catch (e) {}

        var iconSettings = {
            mapIconUrl: '<svg transform="translate(-30,-60)" width="58" height="71" ' +             // Подгоняем маркер к координатам
                        'style="width: 58px; height: 71px; transform: translate(-30px,-60px);"> ' + // Для планшета (атрибут не поддерживается)
                        '<g transform="scale(0.7)">' +
                        '<ellipse cx="51" cy="96" rx="10" ry="5" fill="grey" style="cursor: pointer;" opacity="0.6"></ellipse>' +
                        '<path fill="{mapIconColor}" d="M 59.593937 0.4826568 A 24.067799 23.533833 47.23309 0 0 47.348356 31.57908 ' +
                        'A 24.067799 23.533833 47.23309 0 0 78.671954 44.544372 ' +
                        'A 24.067799 23.533833 47.23309 0 0 79.887013 43.996453 ' +
                        'A 19.568363 1.1313482 82.519857 0 0 80.878789 52.415658 ' +
                        'A 19.568363 1.1313482 82.519857 0 0 83.975043 70.439899 ' +
                        'A 2.5175273 25.119949 9.0413241 0 0 84.542804 71.789906 ' +
                        'A 2.5175273 25.119949 9.0413241 0 0 90.973935 47.425829 ' +
                        'A 2.5175273 25.119949 9.0413241 0 0 92.913413 22.971299 '+
                        'A 24.067799 23.533833 47.23309 0 0 90.917742 13.448427 ' +
                        'A 24.067799 23.533833 47.23309 0 0 59.593937 0.4826568"' +
                        'transform="matrix(0.92443322,0.38134397,-0.40026478,0.91639953,0,0)"/>' +
                        '</g>' +
                        '</svg>' +
                        '<i class="{mapIconPictogram}" ' +
                        'style="top: -52px; left: -16px; font-size: 16px; color: white;"' +         // Подгоняем пиктограмму в центр иконки
                        '></i>',
            mapIconColor: customColor ? customColor : '#2c84cb',
            mapIconTitle: sum,
            mapIconPictogram: customPictogram ? customPictogram : ""
        };

        var defaultIcon = L.divIcon({
            className: "map-default-marker",
            html: sum ? L.Util.template(iconSettings.mapIconUrl, iconSettings) : null,
        });
        return defaultIcon;
    }
});
/*}*/

/*{ Добавление новых маркеров для карт Несколько кругов  mapIconTitle*/
var maps = window.NetDB.namespace('maps');

maps.registerMarkerType({
    slug: 'several_circle',
    name: 'Несколько кругов',
    size: 60,
    icon: function (cluster, features, legend) {
        let data = features.collectProperties(cluster.getAllChildMarkers()),
            childMarkers = cluster.getAllChildMarkers(),
            customColor,
            sum,
            value,
            colors = features._initColors();
        _.each(data, prop => {
            if (prop.value) {
                customColor = prop.color || colors[prop.name];
                sum = (sum || 0) + prop.value;
                value = prop.value;
            }
          console.log('prop', prop);
          console.log('childMarkers',childMarkers);
        });
        console.log('data', data);
        var iconSettings = {
            mapIconUrl: '<svg xmlns="http://www.w3.org/2000/svg" transform="translate(-20,-40)" width="48" height="48" viewBox="0 0 48 48"' +
                        ' style="width: 48px; height: 48px; transform: translate(-20px,-40px);" >' +
                        '<g transform="scale(0.6)"><ellipse cx="41" cy="75" rx="10" ry="5" fill="grey" style="cursor: pointer;" opacity="0.6"></ellipse>' +
                        '<path fill="#2c84cb" d="M38.9988 73.2875L40.7863 75.2375L42.7363 73.2875C43.7113 71.9875 67.5988 44.85 67.5988 30.7125C67.5988' +
                        '15.925 55.5738 4.0625 40.9488 4.0625C26.1613 4.0625 14.2988 16.0875 14.2988 30.7125C14.2988 44.85 38.1863 72.15 38.9988 73.2875Z" style=""></path>' +
                        '<circle cx="41" cy="31" r="19" fill="white"></circle>' +
                        '</g>' +
                        '{mapIconTitle}' +
                        '</svg>',
            mapIconColor: customColor ? customColor : '#2c84cb',
            mapIconTitle: (data[0] ? '<div class="label_tp">'+(childMarkers.length == 1 ? data[0].label: '')+'<span class="markerAbove markerAboveOne" style="background:' + (data[0].value > 0 ? data[0].color : '#8fce00') + ';">' + (data[0].value > 0 ? data[0].value : '') +
                   '</span>' : '') + (data[1] ? '<span class="markerAbove markerAboveTwo" style="background:' + (data[1].value > 0 ? data[1].color : '#8fce00') + ';">' +  (data[1].value > 0 ? data[1].value : '') +
                   '</span>' : '') + (data[2] ? '<span class="markerAbove markerAboveThree" style="background:' +  (data[2].value > 0 ? data[2].color : '#8fce00') + ';">' +  (data[2].value > 0 ? data[2].value : '') +
                   '</span>' : '') + (data[3] ? '<span class="markerAbove markerAboveFour" style="background:' + (data[3].value > 0 ? data[3].color : '#8fce00') + ';">' +  (data[3].value > 0 ? data[3].value : '') +
                    '</span></div>'+ (childMarkers.length > 1 ? '<div class="count_child_markers">'+childMarkers.length+'</div>':'') : '')
        };

        var defaultIcon = L.divIcon({
            className: "map-ground-marker",
            html: L.Util.template(iconSettings.mapIconUrl, iconSettings),
        });
        return defaultIcon;
    }
});
/*}*/
