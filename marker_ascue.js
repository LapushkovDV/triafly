/*{ Добавление новых маркеров «Полигон», «Горячие точки», ... для карт */
var maps = window.NetDB.namespace('maps');

maps.registerMarkerType({
    slug: 'ascue_circles',
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

/*}*/