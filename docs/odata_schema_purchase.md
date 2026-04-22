# OData Schema — ключевые сущности для Procurement UPD

Источник: http://185.126.95.33:81/NB_KA/odata/standard.odata/$metadata

Всего EntityTypes в схеме: 4661

## `StandardODATA.Catalog_ВидыНоменклатуры`

Полей: 127

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `Predefined` | `Edm.Boolean` | true |
| `PredefinedDataName` | `Edm.String` | true |
| `DataVersion` | `Edm.String` | true |
| `Description` | `Edm.String` | true |
| `Parent_Key` | `Edm.Guid` | true |
| `IsFolder` | `Edm.Boolean` | true |
| `DeletionMark` | `Edm.Boolean` | true |
| `АлкогольнаяПродукция` | `Edm.Boolean` | true |
| `ВариантОформленияПродажи` | `Edm.String` | true |
| `ВариантПредставленияНабораВПечатныхФормах` | `Edm.String` | true |
| `ВариантРасчетаЦеныНабора` | `Edm.String` | true |
| `ВестиУчетПоГТД` | `Edm.Boolean` | true |
| `ВестиУчетСертификатовНоменклатуры` | `Edm.Boolean` | true |
| `ВидАлкогольнойПродукции_Key` | `Edm.Guid` | true |
| `ВладелецСерий_Key` | `Edm.Guid` | true |
| `ВладелецТоварныхКатегорий_Key` | `Edm.Guid` | true |
| `ВладелецХарактеристик_Key` | `Edm.Guid` | true |
| `ГруппаАналитическогоУчета_Key` | `Edm.Guid` | true |
| `ГруппаДоступа_Key` | `Edm.Guid` | true |
| `ГруппаФинансовогоУчета_Key` | `Edm.Guid` | true |
| `ЕдиницаДляОтчетов_Key` | `Edm.Guid` | true |
| `ЕдиницаИзмерения_Key` | `Edm.Guid` | true |
| `ЕдиницаИзмеренияСрокаГодности` | `Edm.String` | true |
| `ЗапретРедактированияНаименованияДляПечатиНоменклатуры` | `Edm.Boolean` | true |
| `ЗапретРедактированияНаименованияДляПечатиХарактеристики` | `Edm.Boolean` | true |
| `ЗапретРедактированияРабочегоНаименованияНоменклатуры` | `Edm.Boolean` | true |
| `ЗапретРедактированияРабочегоНаименованияХарактеристики` | `Edm.Boolean` | true |
| `ИмпортнаяАлкогольнаяПродукция` | `Edm.Boolean` | true |
| `ИспользованиеХарактеристик` | `Edm.String` | true |
| `ИспользоватьИндивидуальноеНаименованиеПриПечати` | `Edm.Boolean` | true |
| `ИспользоватьКоличествоСерии` | `Edm.Boolean` | true |
| `ИспользоватьНомерСерии` | `Edm.Boolean` | true |
| `ИспользоватьСерии` | `Edm.Boolean` | true |
| `ИспользоватьСрокГодностиСерии` | `Edm.Boolean` | true |
| `ИспользоватьУпаковки` | `Edm.Boolean` | true |
| `ИспользоватьХарактеристики` | `Edm.Boolean` | true |
| `КонтролироватьДублиНоменклатуры` | `Edm.Boolean` | true |
| `КонтролироватьДублиХарактеристик` | `Edm.Boolean` | true |
| `КоэффициентЕдиницыДляОтчетов` | `Edm.Double` | true |
| `НаборСвойств_Key` | `Edm.Guid` | true |
| `НаборСвойствСерий_Key` | `Edm.Guid` | true |
| `НаборСвойствХарактеристик_Key` | `Edm.Guid` | true |
| `НаборУпаковок_Key` | `Edm.Guid` | true |
| `НаименованиеДляПечати` | `Edm.String` | true |
| `НастройкаИспользованияСерий` | `Edm.String` | true |
| `НоменклатураМногооборотнаяТара_Key` | `Edm.Guid` | true |
| `ОбособленнаяЗакупкаПродажа` | `Edm.Boolean` | true |
| `НастройкиСерийБерутсяИзДругогоВидаНоменклатуры` | `Edm.Boolean` | true |
| `Описание` | `Edm.String` | true |
| `ПодакцизныйТовар` | `Edm.Boolean` | true |
| `ПоставляетсяВМногооборотнойТаре` | `Edm.Boolean` | true |
| `СезоннаяГруппа_Key` | `Edm.Guid` | true |
| `ПолитикаУчетаСерий_Key` | `Edm.Guid` | true |
| `СкладскаяГруппа_Key` | `Edm.Guid` | true |
| `СодержитДрагоценныеМатериалы` | `Edm.Boolean` | true |
| `ТипНоменклатуры` | `Edm.String` | true |
| `ТочностьУказанияСрокаГодностиСерии` | `Edm.String` | true |
| `СтавкаНДС_Key` | `Edm.Guid` | true |
| `ХарактеристикаМногооборотнаяТара_Key` | `Edm.Guid` | true |
| `ТоварныеКатегорииОбщиеСДругимВидомНоменклатуры` | `Edm.Boolean` | true |
| `ШаблонНаименованияДляПечатиНоменклатуры` | `Edm.String` | true |
| `ШаблонНаименованияДляПечатиХарактеристики` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияНоменклатуры` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияСерии` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияХарактеристики` | `Edm.String` | true |
| `ШаблонЦенника_Key` | `Edm.Guid` | true |
| `ШаблонЭтикетки_Key` | `Edm.Guid` | true |
| `ШаблонЭтикеткиСерии_Key` | `Edm.Guid` | true |
| `КодОКВЭД_Key` | `Edm.Guid` | true |
| `КодТНВЭД_Key` | `Edm.Guid` | true |
| `КодОКП_Key` | `Edm.Guid` | true |
| `СхемаОбеспечения_Key` | `Edm.Guid` | true |
| `СпособОбеспеченияПотребностей_Key` | `Edm.Guid` | true |
| `ЦеноваяГруппа_Key` | `Edm.Guid` | true |
| `Крепость` | `Edm.Double` | true |
| `ОсобенностьУчета` | `Edm.String` | true |
| `ПродукцияМаркируемаяДляГИСМ` | `Edm.Boolean` | true |
| `КиЗГИСМ` | `Edm.Boolean` | true |
| `ИспользоватьRFIDМеткиСерии` | `Edm.Boolean` | true |
| `ИспользоватьНомерКИЗГИСМСерии` | `Edm.Boolean` | true |
| `ПодконтрольнаяПродукцияВЕТИС` | `Edm.Boolean` | true |
| `КодРаздел7ДекларацииНДС_Key` | `Edm.Guid` | true |
| `АвтоматическиГенерироватьСерии` | `Edm.Boolean` | true |
| `ИспользоватьДатуПроизводстваСерии` | `Edm.Boolean` | true |
| `ИспользоватьПроизводителяЕГАИССерии` | `Edm.Boolean` | true |
| `ИспользоватьСправку2ЕГАИССерии` | `Edm.Boolean` | true |
| `ИспользоватьПроизводителяВЕТИССерии` | `Edm.Boolean` | true |
| `ИспользоватьЗаписьСкладскогоЖурналаВЕТИССерии` | `Edm.Boolean` | true |
| `ИспользоватьИдентификаторПартииВЕТИССерии` | `Edm.Boolean` | true |
| `ОблагаетсяНДСУПокупателя` | `Edm.Boolean` | true |
| `УдалитьКодОКВЭД2_Key` | `Edm.Guid` | true |
| `КодОКПД2_Key` | `Edm.Guid` | true |
| `ИспользоватьМРЦМОТПСерии` | `Edm.Boolean` | true |
| `EVG_Склад_Key` | `Edm.Guid` | true |
| `EVG_НаправлениеВыпуска` | `Edm.String` | true |
| `bsg_ВидПланаПроизводства_Key` | `Edm.Guid` | true |
| `ДатаПереносаВАрхив` | `Edm.DateTime` | true |
| `НастройкиКлючаЦенПоНоменклатуре` | `Edm.String` | true |
| `НастройкиКлючаЦенПоХарактеристике` | `Edm.String` | true |
| `НастройкиКлючаЦенПоСерии` | `Edm.String` | true |
| `НастройкиКлючаЦенПоУпаковке` | `Edm.String` | true |
| `ШаблонНаименованияДляПечатиКлючаЦен` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияКлючаЦен` | `Edm.String` | true |
| `РазрезыЦенообразования` | `Edm.String` | true |
| `ШаблонНаименованияДляПечатиНоменклатурыИсторияПереходаНаНовыеФормулы` | `Edm.String` | true |
| `ШаблонНаименованияДляПечатиХарактеристикиИсторияПереходаНаНовыеФормулы` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияНоменклатурыИсторияПереходаНаНовыеФормулы` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияСерииИсторияПереходаНаНовыеФормулы` | `Edm.String` | true |
| `ШаблонРабочегоНаименованияХарактеристикиИсторияПереходаНаНовыеФормулы` | `Edm.String` | true |
| `bsg_ПодразделениеСписания_Key` | `Edm.Guid` | true |
| `ЗапрещенаПродажаЧерезПатент` | `Edm.Boolean` | true |
| `КодОКВЭД2` | `Edm.String` | true |
| `РеквизитыДляКонтроляНоменклатуры` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыДляКонтроляНоменклатуры_RowType)` | true |
| `РеквизитыДляКонтроляХарактеристик` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыДляКонтроляХарактеристик_RowType)` | true |
| `РеквизитыДляКонтроляСерий` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыДляКонтроляСерий_RowType)` | true |
| `РеквизитыБыстрогоОтбораНоменклатуры` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыБыстрогоОтбораНоменклатуры_RowType)` | true |
| `РеквизитыБыстрогоОтбораХарактеристик` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыБыстрогоОтбораХарактеристик_RowType)` | true |
| `ПолитикиУчетаСерий` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_ПолитикиУчетаСерий_RowType)` | true |
| `EVG_ТЧПодразделение` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_EVG_ТЧПодразделение_RowType)` | true |
| `EVG_ТЧЦеховаяКладовая` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_EVG_ТЧЦеховаяКладовая_RowType)` | true |
| `EVG_ТЧПодразделениеВыпуск` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_EVG_ТЧПодразделениеВыпуск_RowType)` | true |
| `EVG_ТЧСкладВыпуск` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_EVG_ТЧСкладВыпуск_RowType)` | true |
| `EVG_ТЧСкладОтправительПеремещение` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_EVG_ТЧСкладОтправительПеремещение_RowType)` | true |
| `EVG_ТЧСкладПолучательПеремещение` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_EVG_ТЧСкладПолучательПеремещение_RowType)` | true |
| `РеквизитыХарактеристикДляКлючаЦен` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыХарактеристикДляКлючаЦен_RowType)` | true |
| `РеквизитыСерийДляКлючаЦен` | `Collection(StandardODATA.Catalog_ВидыНоменклатуры_РеквизитыСерийДляКлючаЦен_RowType)` | true |

**Navigation** (30):
- `Parent` → `StandardODATA.Catalog_ВидыНоменклатуры_Parent`
- `ВладелецСерий` → `StandardODATA.Catalog_ВидыНоменклатуры_ВладелецСерий`
- `ВладелецТоварныхКатегорий` → `StandardODATA.Catalog_ВидыНоменклатуры_ВладелецТоварныхКатегорий`
- `ВладелецХарактеристик` → `StandardODATA.Catalog_ВидыНоменклатуры_ВладелецХарактеристик`
- `ГруппаАналитическогоУчета` → `StandardODATA.Catalog_ВидыНоменклатуры_ГруппаАналитическогоУчета`
- `ГруппаФинансовогоУчета` → `StandardODATA.Catalog_ВидыНоменклатуры_ГруппаФинансовогоУчета`
- `ЕдиницаДляОтчетов` → `StandardODATA.Catalog_ВидыНоменклатуры_ЕдиницаДляОтчетов`
- `ЕдиницаИзмерения` → `StandardODATA.Catalog_ВидыНоменклатуры_ЕдиницаИзмерения`
- `НаборСвойств` → `StandardODATA.Catalog_ВидыНоменклатуры_НаборСвойств`
- `НаборСвойствСерий` → `StandardODATA.Catalog_ВидыНоменклатуры_НаборСвойствСерий`
- `НаборСвойствХарактеристик` → `StandardODATA.Catalog_ВидыНоменклатуры_НаборСвойствХарактеристик`
- `НаборУпаковок` → `StandardODATA.Catalog_ВидыНоменклатуры_НаборУпаковок`
- `НоменклатураМногооборотнаяТара` → `StandardODATA.Catalog_ВидыНоменклатуры_НоменклатураМногооборотнаяТара`
- `СезоннаяГруппа` → `StandardODATA.Catalog_ВидыНоменклатуры_СезоннаяГруппа`
- `ПолитикаУчетаСерий` → `StandardODATA.Catalog_ВидыНоменклатуры_ПолитикаУчетаСерий`
- `СтавкаНДС` → `StandardODATA.Catalog_ВидыНоменклатуры_СтавкаНДС`
- `ШаблонЦенника` → `StandardODATA.Catalog_ВидыНоменклатуры_ШаблонЦенника`
- `ШаблонЭтикетки` → `StandardODATA.Catalog_ВидыНоменклатуры_ШаблонЭтикетки`
- `ШаблонЭтикеткиСерии` → `StandardODATA.Catalog_ВидыНоменклатуры_ШаблонЭтикеткиСерии`
- `КодОКВЭД` → `StandardODATA.Catalog_ВидыНоменклатуры_КодОКВЭД`
- `КодОКП` → `StandardODATA.Catalog_ВидыНоменклатуры_КодОКП`
- `СхемаОбеспечения` → `StandardODATA.Catalog_ВидыНоменклатуры_СхемаОбеспечения`
- `СпособОбеспеченияПотребностей` → `StandardODATA.Catalog_ВидыНоменклатуры_СпособОбеспеченияПотребностей`
- `ЦеноваяГруппа` → `StandardODATA.Catalog_ВидыНоменклатуры_ЦеноваяГруппа`
- `КодРаздел7ДекларацииНДС` → `StandardODATA.Catalog_ВидыНоменклатуры_КодРаздел7ДекларацииНДС`
- `УдалитьКодОКВЭД2` → `StandardODATA.Catalog_ВидыНоменклатуры_УдалитьКодОКВЭД2`
- `КодОКПД2` → `StandardODATA.Catalog_ВидыНоменклатуры_КодОКПД2`
- `EVG_Склад` → `StandardODATA.Catalog_ВидыНоменклатуры_EVG_Склад`
- `bsg_ВидПланаПроизводства` → `StandardODATA.Catalog_ВидыНоменклатуры_bsg_ВидПланаПроизводства`
- `bsg_ПодразделениеСписания` → `StandardODATA.Catalog_ВидыНоменклатуры_bsg_ПодразделениеСписания`

## `StandardODATA.Catalog_СерииНоменклатуры`

Полей: 25

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `Predefined` | `Edm.Boolean` | true |
| `PredefinedDataName` | `Edm.String` | true |
| `DataVersion` | `Edm.String` | true |
| `Description` | `Edm.String` | true |
| `DeletionMark` | `Edm.Boolean` | true |
| `ГоденДо` | `Edm.DateTime` | true |
| `ВидНоменклатуры_Key` | `Edm.Guid` | true |
| `Номер` | `Edm.String` | true |
| `НомерКиЗГИСМ` | `Edm.String` | true |
| `RFIDTID` | `Edm.String` | true |
| `RFIDUser` | `Edm.String` | true |
| `RFIDEPC` | `Edm.String` | true |
| `EPCGTIN` | `Edm.String` | true |
| `RFIDМеткаНеЧитаемая` | `Edm.Boolean` | true |
| `ДатаПроизводства` | `Edm.DateTime` | true |
| `ПроизводительЕГАИС_Key` | `Edm.Guid` | true |
| `Справка2ЕГАИС_Key` | `Edm.Guid` | true |
| `ПроизводительВЕТИС_Key` | `Edm.Guid` | true |
| `ЗаписьСкладскогоЖурналаВЕТИС_Key` | `Edm.Guid` | true |
| `ИдентификаторПартииВЕТИС` | `Edm.String` | true |
| `МаксимальнаяРозничнаяЦенаМОТП` | `Edm.Double` | true |
| `СерияНоменклатурыДляЦенообразования_Key` | `Edm.Guid` | true |
| `bsg_Отдатировано` | `Edm.Boolean` | true |
| `ДополнительныеРеквизиты` | `Collection(StandardODATA.Catalog_СерииНоменклатуры_ДополнительныеРеквизиты_RowType)` | true |

**Navigation** (4):
- `ВидНоменклатуры` → `StandardODATA.Catalog_СерииНоменклатуры_ВидНоменклатуры`
- `ПроизводительВЕТИС` → `StandardODATA.Catalog_СерииНоменклатуры_ПроизводительВЕТИС`
- `ЗаписьСкладскогоЖурналаВЕТИС` → `StandardODATA.Catalog_СерииНоменклатуры_ЗаписьСкладскогоЖурналаВЕТИС`
- `СерияНоменклатурыДляЦенообразования` → `StandardODATA.Catalog_СерииНоменклатуры_СерияНоменклатурыДляЦенообразования`

## `StandardODATA.Catalog_СоглашенияСПоставщиками`

Полей: 70

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `Predefined` | `Edm.Boolean` | true |
| `PredefinedDataName` | `Edm.String` | true |
| `DataVersion` | `Edm.String` | true |
| `Description` | `Edm.String` | true |
| `DeletionMark` | `Edm.Boolean` | true |
| `Номер` | `Edm.String` | true |
| `Дата` | `Edm.DateTime` | true |
| `Контрагент_Key` | `Edm.Guid` | true |
| `Партнер_Key` | `Edm.Guid` | true |
| `Организация_Key` | `Edm.Guid` | true |
| `Валюта_Key` | `Edm.Guid` | true |
| `ЦенаВключаетНДС` | `Edm.Boolean` | true |
| `СрокПоставки` | `Edm.Int64` | true |
| `Склад_Key` | `Edm.Guid` | true |
| `ДатаНачалаДействия` | `Edm.DateTime` | true |
| `ДатаОкончанияДействия` | `Edm.DateTime` | true |
| `Комментарий` | `Edm.String` | true |
| `Статус` | `Edm.String` | true |
| `Согласован` | `Edm.Boolean` | true |
| `Менеджер_Key` | `Edm.Guid` | true |
| `СпособРасчетаВознаграждения` | `Edm.String` | true |
| `ПроцентВознаграждения` | `Edm.Double` | true |
| `УдержатьВознаграждение` | `Edm.Boolean` | true |
| `ХозяйственнаяОперация` | `Edm.String` | true |
| `ПроцентРучнойСкидки` | `Edm.Double` | true |
| `ПроцентРучнойНаценки` | `Edm.Double` | true |
| `КонтролироватьЦеныЗакупки` | `Edm.Boolean` | true |
| `ФормаОплаты` | `Edm.String` | true |
| `ВариантПриемкиТоваров` | `Edm.String` | true |
| `ГруппаФинансовогоУчета_Key` | `Edm.Guid` | true |
| `РегистрироватьЦеныПоставщика` | `Edm.Boolean` | true |
| `ИспользуютсяДоговорыКонтрагентов` | `Edm.Boolean` | true |
| `ПорядокРасчетов` | `Edm.String` | true |
| `ВозвращатьМногооборотнуюТару` | `Edm.Boolean` | true |
| `СрокВозвратаМногооборотнойТары` | `Edm.Int16` | true |
| `РассчитыватьДатуВозвратаТарыПоКалендарю` | `Edm.Boolean` | true |
| `Календарь_Key` | `Edm.Guid` | true |
| `ТребуетсяЗалогЗаТару` | `Edm.Boolean` | true |
| `КалендарьВозвратаТары_Key` | `Edm.Guid` | true |
| `СтатьяДвиженияДенежныхСредств_Key` | `Edm.Guid` | true |
| `ИспользоватьУказанныеАгентскиеУслуги` | `Edm.Boolean` | true |
| `ВидЦеныПоставщика_Key` | `Edm.Guid` | true |
| `КодНаименованияСделки` | `Edm.String` | true |
| `СпособОпределенияЦеныСделки_Key` | `Edm.Guid` | true |
| `КодУсловийПоставки` | `Edm.String` | true |
| `НаправлениеДеятельности_Key` | `Edm.Guid` | true |
| `ВалютаВзаиморасчетов_Key` | `Edm.Guid` | true |
| `СпособДоставки` | `Edm.String` | true |
| `ПеревозчикПартнер_Key` | `Edm.Guid` | true |
| `ЗонаДоставки_Key` | `Edm.Guid` | true |
| `ВремяДоставкиС` | `Edm.DateTime` | true |
| `ВремяДоставкиПо` | `Edm.DateTime` | true |
| `АдресДоставки` | `Edm.String` | true |
| `АдресДоставкиЗначенияПолей` | `Edm.String` | true |
| `ДополнительнаяИнформацияПоДоставке` | `Edm.String` | true |
| `АдресДоставкиПеревозчика` | `Edm.String` | true |
| `АдресДоставкиПеревозчикаЗначенияПолей` | `Edm.String` | true |
| `АдресДоставкиДляПоставщика` | `Edm.String` | true |
| `ОплатаВВалюте` | `Edm.Boolean` | true |
| `МинимальнаяСуммаЗаказа` | `Edm.Double` | true |
| `АдресДоставкиЗначение` | `Edm.String` | true |
| `АдресДоставкиПеревозчикаЗначение` | `Edm.String` | true |
| `СрокПереходаПраваСобственности` | `Edm.Int16` | true |
| `СрокДоставки` | `Edm.Int16` | true |
| `РазбиватьОтчетПоДокументам` | `Edm.Boolean` | true |
| `ПереоцениватьУслугиКОтчетуКомитенту` | `Edm.Boolean` | true |
| `ДополнительныеРеквизиты` | `Collection(StandardODATA.Catalog_СоглашенияСПоставщиками_ДополнительныеРеквизиты_RowType)` | true |
| `ЭтапыГрафикаОплаты` | `Collection(StandardODATA.Catalog_СоглашенияСПоставщиками_ЭтапыГрафикаОплаты_RowType)` | true |
| `АгентскиеУслуги` | `Collection(StandardODATA.Catalog_СоглашенияСПоставщиками_АгентскиеУслуги_RowType)` | true |

**Navigation** (15):
- `Контрагент` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Контрагент`
- `Партнер` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Партнер`
- `Организация` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Организация`
- `Валюта` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Валюта`
- `Склад` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Склад`
- `Менеджер` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Менеджер`
- `ГруппаФинансовогоУчета` → `StandardODATA.Catalog_СоглашенияСПоставщиками_ГруппаФинансовогоУчета`
- `Календарь` → `StandardODATA.Catalog_СоглашенияСПоставщиками_Календарь`
- `КалендарьВозвратаТары` → `StandardODATA.Catalog_СоглашенияСПоставщиками_КалендарьВозвратаТары`
- `СтатьяДвиженияДенежныхСредств` → `StandardODATA.Catalog_СоглашенияСПоставщиками_СтатьяДвиженияДенежныхСредств`
- `ВидЦеныПоставщика` → `StandardODATA.Catalog_СоглашенияСПоставщиками_ВидЦеныПоставщика`
- `НаправлениеДеятельности` → `StandardODATA.Catalog_СоглашенияСПоставщиками_НаправлениеДеятельности`
- `ВалютаВзаиморасчетов` → `StandardODATA.Catalog_СоглашенияСПоставщиками_ВалютаВзаиморасчетов`
- `ПеревозчикПартнер` → `StandardODATA.Catalog_СоглашенияСПоставщиками_ПеревозчикПартнер`
- `ЗонаДоставки` → `StandardODATA.Catalog_СоглашенияСПоставщиками_ЗонаДоставки`

## `StandardODATA.Document_ЗаказПоставщику`

Полей: 80

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `DataVersion` | `Edm.String` | true |
| `Number` | `Edm.String` | true |
| `Date` | `Edm.DateTime` | true |
| `DeletionMark` | `Edm.Boolean` | true |
| `Posted` | `Edm.Boolean` | true |
| `Партнер_Key` | `Edm.Guid` | true |
| `Контрагент_Key` | `Edm.Guid` | true |
| `Организация_Key` | `Edm.Guid` | true |
| `Соглашение_Key` | `Edm.Guid` | true |
| `Склад_Key` | `Edm.Guid` | true |
| `Валюта_Key` | `Edm.Guid` | true |
| `Менеджер_Key` | `Edm.Guid` | true |
| `ЦенаВключаетНДС` | `Edm.Boolean` | true |
| `Статус` | `Edm.String` | true |
| `СуммаДокумента` | `Edm.Double` | true |
| `ДополнительнаяИнформация` | `Edm.String` | true |
| `ЖелаемаяДатаПоступления` | `Edm.DateTime` | true |
| `МаксимальныйКодСтроки` | `Edm.Int64` | true |
| `Согласован` | `Edm.Boolean` | true |
| `ФормаОплаты` | `Edm.String` | true |
| `Касса_Key` | `Edm.Guid` | true |
| `БанковскийСчет_Key` | `Edm.Guid` | true |
| `СуммаАвансаДоПодтверждения` | `Edm.Double` | true |
| `СуммаПредоплатыДоПоступления` | `Edm.Double` | true |
| `ДатаПервогоПоступления` | `Edm.DateTime` | true |
| `ДатаСогласования` | `Edm.DateTime` | true |
| `НалогообложениеНДС` | `Edm.String` | true |
| `ХозяйственнаяОперация` | `Edm.String` | true |
| `Комментарий` | `Edm.String` | true |
| `НомерПоДаннымПоставщика` | `Edm.String` | true |
| `ДатаПоДаннымПоставщика` | `Edm.DateTime` | true |
| `Сделка_Key` | `Edm.Guid` | true |
| `Подразделение_Key` | `Edm.Guid` | true |
| `ГруппаФинансовогоУчета_Key` | `Edm.Guid` | true |
| `РегистрироватьЦеныПоставщика` | `Edm.Boolean` | true |
| `Договор_Key` | `Edm.Guid` | true |
| `Автор_Key` | `Edm.Guid` | true |
| `ДокументОснование` | `Edm.String` | true |
| `ПоступлениеОднойДатой` | `Edm.Boolean` | true |
| `ДатаПоступления` | `Edm.DateTime` | true |
| `ПорядокРасчетов` | `Edm.String` | true |
| `АдресДоставкиДляПоставщика` | `Edm.String` | true |
| `КонтактноеЛицо_Key` | `Edm.Guid` | true |
| `ВернутьМногооборотнуюТару` | `Edm.Boolean` | true |
| `СрокВозвратаМногооборотнойТары` | `Edm.Int16` | true |
| `СостояниеЗаполненияМногооборотнойТары` | `Edm.String` | true |
| `ЗакупкаПодДеятельность` | `Edm.String` | true |
| `ТребуетсяЗалогЗаТару` | `Edm.Boolean` | true |
| `СуммаВозвратнойТары` | `Edm.Double` | true |
| `Приоритет_Key` | `Edm.Guid` | true |
| `СпособДоставки` | `Edm.String` | true |
| `ПеревозчикПартнер_Key` | `Edm.Guid` | true |
| `ЗонаДоставки_Key` | `Edm.Guid` | true |
| `ВремяДоставкиС` | `Edm.DateTime` | true |
| `ВремяДоставкиПо` | `Edm.DateTime` | true |
| `АдресДоставки` | `Edm.String` | true |
| `АдресДоставкиЗначенияПолей` | `Edm.String` | true |
| `ДополнительнаяИнформацияПоДоставке` | `Edm.String` | true |
| `АдресДоставкиПеревозчика` | `Edm.String` | true |
| `АдресДоставкиПеревозчикаЗначенияПолей` | `Edm.String` | true |
| `ОсобыеУсловияПеревозки` | `Edm.Boolean` | true |
| `ОсобыеУсловияПеревозкиОписание` | `Edm.String` | true |
| `НаправлениеДеятельности_Key` | `Edm.Guid` | true |
| `ЕстьКиЗГИСМ` | `Edm.Boolean` | true |
| `ВариантПриемкиТоваров` | `Edm.String` | true |
| `EVG_СтатусЗакрыт` | `Edm.Boolean` | true |
| `EVG_СтатьяДДС_Key` | `Edm.Guid` | true |
| `ОплатаВВалюте` | `Edm.Boolean` | true |
| `ОбъектРасчетов_Key` | `Edm.Guid` | true |
| `АдресДоставкиЗначение` | `Edm.String` | true |
| `АдресДоставкиПеревозчикаЗначение` | `Edm.String` | true |
| `ДатаОтгрузки` | `Edm.DateTime` | true |
| `ДлительностьДоставки` | `Edm.Int16` | true |
| `BSG_ПланЗакупок_Key` | `Edm.Guid` | true |
| `ОперацияССамозанятым` | `Edm.Boolean` | true |
| `Товары` | `Collection(StandardODATA.Document_ЗаказПоставщику_Товары_RowType)` | true |
| `ЭтапыГрафикаОплаты` | `Collection(StandardODATA.Document_ЗаказПоставщику_ЭтапыГрафикаОплаты_RowType)` | true |
| `ДополнительныеРеквизиты` | `Collection(StandardODATA.Document_ЗаказПоставщику_ДополнительныеРеквизиты_RowType)` | true |
| `ДокументОснование_Type` | `Edm.String` | true |

**Navigation** (22):
- `Партнер` → `StandardODATA.Document_ЗаказПоставщику_Партнер`
- `Контрагент` → `StandardODATA.Document_ЗаказПоставщику_Контрагент`
- `Организация` → `StandardODATA.Document_ЗаказПоставщику_Организация`
- `Соглашение` → `StandardODATA.Document_ЗаказПоставщику_Соглашение`
- `Склад` → `StandardODATA.Document_ЗаказПоставщику_Склад`
- `Валюта` → `StandardODATA.Document_ЗаказПоставщику_Валюта`
- `Менеджер` → `StandardODATA.Document_ЗаказПоставщику_Менеджер`
- `Касса` → `StandardODATA.Document_ЗаказПоставщику_Касса`
- `БанковскийСчет` → `StandardODATA.Document_ЗаказПоставщику_БанковскийСчет`
- `Сделка` → `StandardODATA.Document_ЗаказПоставщику_Сделка`
- `Подразделение` → `StandardODATA.Document_ЗаказПоставщику_Подразделение`
- `ГруппаФинансовогоУчета` → `StandardODATA.Document_ЗаказПоставщику_ГруппаФинансовогоУчета`
- `Договор` → `StandardODATA.Document_ЗаказПоставщику_Договор`
- `Автор` → `StandardODATA.Document_ЗаказПоставщику_Автор`
- `КонтактноеЛицо` → `StandardODATA.Document_ЗаказПоставщику_КонтактноеЛицо`
- `Приоритет` → `StandardODATA.Document_ЗаказПоставщику_Приоритет`
- `ПеревозчикПартнер` → `StandardODATA.Document_ЗаказПоставщику_ПеревозчикПартнер`
- `ЗонаДоставки` → `StandardODATA.Document_ЗаказПоставщику_ЗонаДоставки`
- `НаправлениеДеятельности` → `StandardODATA.Document_ЗаказПоставщику_НаправлениеДеятельности`
- `EVG_СтатьяДДС` → `StandardODATA.Document_ЗаказПоставщику_EVG_СтатьяДДС`
- `ОбъектРасчетов` → `StandardODATA.Document_ЗаказПоставщику_ОбъектРасчетов`
- `BSG_ПланЗакупок` → `StandardODATA.Document_ЗаказПоставщику_BSG_ПланЗакупок`

## `StandardODATA.Document_ЗаказПоставщику_Товары`

Полей: 29

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `LineNumber` | `Edm.Int64` | false |
| `НоменклатураПартнера_Key` | `Edm.Guid` | true |
| `Номенклатура_Key` | `Edm.Guid` | true |
| `Характеристика_Key` | `Edm.Guid` | true |
| `Упаковка_Key` | `Edm.Guid` | true |
| `КоличествоУпаковок` | `Edm.Double` | true |
| `Количество` | `Edm.Double` | true |
| `ДатаПоступления` | `Edm.DateTime` | true |
| `ВидЦеныПоставщика_Key` | `Edm.Guid` | true |
| `Цена` | `Edm.Double` | true |
| `Сумма` | `Edm.Double` | true |
| `ПроцентРучнойСкидки` | `Edm.Double` | true |
| `СуммаРучнойСкидки` | `Edm.Double` | true |
| `СтавкаНДС_Key` | `Edm.Guid` | true |
| `СуммаНДС` | `Edm.Double` | true |
| `СуммаСНДС` | `Edm.Double` | true |
| `КодСтроки` | `Edm.Int64` | true |
| `Отменено` | `Edm.Boolean` | true |
| `СтатьяРасходов_Key` | `Edm.Guid` | true |
| `АналитикаРасходов` | `Edm.String` | true |
| `ПричинаОтмены_Key` | `Edm.Guid` | true |
| `Склад_Key` | `Edm.Guid` | true |
| `Назначение_Key` | `Edm.Guid` | true |
| `Подразделение_Key` | `Edm.Guid` | true |
| `СписатьНаРасходы` | `Edm.Boolean` | true |
| `ИдентификаторСтроки` | `Edm.String` | true |
| `ДатаОтгрузки` | `Edm.DateTime` | true |
| `АналитикаРасходов_Type` | `Edm.String` | true |

**Navigation** (8):
- `НоменклатураПартнера` → `StandardODATA.Document_ЗаказПоставщику_Товары_НоменклатураПартнера`
- `Номенклатура` → `StandardODATA.Document_ЗаказПоставщику_Товары_Номенклатура`
- `Упаковка` → `StandardODATA.Document_ЗаказПоставщику_Товары_Упаковка`
- `ВидЦеныПоставщика` → `StandardODATA.Document_ЗаказПоставщику_Товары_ВидЦеныПоставщика`
- `СтавкаНДС` → `StandardODATA.Document_ЗаказПоставщику_Товары_СтавкаНДС`
- `СтатьяРасходов` → `StandardODATA.Document_ЗаказПоставщику_Товары_СтатьяРасходов`
- `Склад` → `StandardODATA.Document_ЗаказПоставщику_Товары_Склад`
- `Подразделение` → `StandardODATA.Document_ЗаказПоставщику_Товары_Подразделение`

## `StandardODATA.Document_ПриобретениеТоваровУслуг`

Полей: 95

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `DataVersion` | `Edm.String` | true |
| `Number` | `Edm.String` | true |
| `Date` | `Edm.DateTime` | true |
| `DeletionMark` | `Edm.Boolean` | true |
| `Posted` | `Edm.Boolean` | true |
| `Валюта_Key` | `Edm.Guid` | true |
| `Партнер_Key` | `Edm.Guid` | true |
| `ХозяйственнаяОперация` | `Edm.String` | true |
| `Подразделение_Key` | `Edm.Guid` | true |
| `Склад_Key` | `Edm.Guid` | true |
| `Контрагент_Key` | `Edm.Guid` | true |
| `СуммаДокумента` | `Edm.Double` | true |
| `СуммаВзаиморасчетовПоЗаказу` | `Edm.Double` | true |
| `Менеджер_Key` | `Edm.Guid` | true |
| `ЗаказПоставщику_Key` | `Edm.Guid` | true |
| `ПодотчетноеЛицо_Key` | `Edm.Guid` | true |
| `ЦенаВключаетНДС` | `Edm.Boolean` | true |
| `ВалютаВзаиморасчетов_Key` | `Edm.Guid` | true |
| `Комментарий` | `Edm.String` | true |
| `ЗакупкаПодДеятельность` | `Edm.String` | true |
| `ФормаОплаты` | `Edm.String` | true |
| `Согласован` | `Edm.Boolean` | true |
| `НалогообложениеНДС` | `Edm.String` | true |
| `СуммаВзаиморасчетов` | `Edm.Double` | true |
| `БанковскийСчетОрганизации_Key` | `Edm.Guid` | true |
| `НомерВходящегоДокумента` | `Edm.String` | true |
| `ДатаВходящегоДокумента` | `Edm.DateTime` | true |
| `Грузоотправитель_Key` | `Edm.Guid` | true |
| `БанковскийСчетКонтрагента_Key` | `Edm.Guid` | true |
| `БанковскийСчетГрузоотправителя_Key` | `Edm.Guid` | true |
| `Сделка_Key` | `Edm.Guid` | true |
| `Принял_Key` | `Edm.Guid` | true |
| `ПринялДолжность` | `Edm.String` | true |
| `ПоступлениеПоЗаказам` | `Edm.Boolean` | true |
| `ГруппаФинансовогоУчета_Key` | `Edm.Guid` | true |
| `РегистрироватьЦеныПоставщика` | `Edm.Boolean` | true |
| `Договор_Key` | `Edm.Guid` | true |
| `Автор_Key` | `Edm.Guid` | true |
| `Руководитель_Key` | `Edm.Guid` | true |
| `ПорядокРасчетов` | `Edm.String` | true |
| `ВернутьМногооборотнуюТару` | `Edm.Boolean` | true |
| `ДатаВозвратаМногооборотнойТары` | `Edm.DateTime` | true |
| `СостояниеЗаполненияМногооборотнойТары` | `Edm.String` | true |
| `ТребуетсяЗалогЗаТару` | `Edm.Boolean` | true |
| `ДопоступлениеПоДокументу` | `Edm.String` | true |
| `НазначениеАванса` | `Edm.String` | true |
| `КоличествоДокументов` | `Edm.String` | true |
| `КоличествоЛистов` | `Edm.String` | true |
| `ГлавныйБухгалтер_Key` | `Edm.Guid` | true |
| `СтатьяДвиженияДенежныхСредств_Key` | `Edm.Guid` | true |
| `СпособДоставки` | `Edm.String` | true |
| `ПеревозчикПартнер_Key` | `Edm.Guid` | true |
| `ЗонаДоставки_Key` | `Edm.Guid` | true |
| `ВремяДоставкиС` | `Edm.DateTime` | true |
| `ВремяДоставкиПо` | `Edm.DateTime` | true |
| `АдресДоставки` | `Edm.String` | true |
| `АдресДоставкиЗначенияПолей` | `Edm.String` | true |
| `ДополнительнаяИнформацияПоДоставке` | `Edm.String` | true |
| `АдресДоставкиПеревозчика` | `Edm.String` | true |
| `АдресДоставкиПеревозчикаЗначенияПолей` | `Edm.String` | true |
| `ОсобыеУсловияПеревозки` | `Edm.Boolean` | true |
| `ОсобыеУсловияПеревозкиОписание` | `Edm.String` | true |
| `НаправлениеДеятельности_Key` | `Edm.Guid` | true |
| `ЕстьАлкогольнаяПродукция` | `Edm.Boolean` | true |
| `Соглашение_Key` | `Edm.Guid` | true |
| `Организация_Key` | `Edm.Guid` | true |
| `КурсЧислитель` | `Edm.Double` | true |
| `КурсЗнаменатель` | `Edm.Double` | true |
| `ЕстьМаркируемаяПродукцияГИСМ` | `Edm.Boolean` | true |
| `ЕстьКиЗГИСМ` | `Edm.Boolean` | true |
| `ВариантПриемкиТоваров` | `Edm.String` | true |
| `СуммаВзаиморасчетовПоТаре` | `Edm.Double` | true |
| `АвансовыйОтчет_Key` | `Edm.Guid` | true |
| `НаименованиеВходящегоДокумента` | `Edm.String` | true |
| `ОплатаВВалюте` | `Edm.Boolean` | true |
| `АдресДоставкиЗначение` | `Edm.String` | true |
| `АдресДоставкиПеревозчикаЗначение` | `Edm.String` | true |
| `BSG_ДокументОснования` | `Edm.String` | true |
| `КорректировкаОстатковРНПТ` | `Edm.Boolean` | true |
| `ДатаПоступления` | `Edm.DateTime` | true |
| `ДатаКурсаВалютыДокумента` | `Edm.DateTime` | true |
| `НоваяМеханикаСозданияЗаявленийОВвозе` | `Edm.Boolean` | true |
| `ОбъектРасчетовУпр_Key` | `Edm.Guid` | true |
| `ОперацияССамозанятым` | `Edm.Boolean` | true |
| `bsg_Согласован` | `Edm.Boolean` | true |
| `Товары` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_RowType)` | true |
| `ДополнительныеРеквизиты` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_ДополнительныеРеквизиты_RowType)` | true |
| `РасшифровкаПлатежа` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_РасшифровкаПлатежа_RowType)` | true |
| `Серии` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_Серии_RowType)` | true |
| `ВидыЗапасов` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_ВидыЗапасов_RowType)` | true |
| `ЭтапыГрафикаОплаты` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_ЭтапыГрафикаОплаты_RowType)` | true |
| `ШтрихкодыУпаковок` | `Collection(StandardODATA.Document_ПриобретениеТоваровУслуг_ШтрихкодыУпаковок_RowType)` | true |
| `ДопоступлениеПоДокументу_Type` | `Edm.String` | true |
| `BSG_ДокументОснования_Type` | `Edm.String` | true |

**Navigation** (28):
- `Валюта` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Валюта`
- `Партнер` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Партнер`
- `Подразделение` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Подразделение`
- `Склад` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Склад`
- `Контрагент` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Контрагент`
- `Менеджер` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Менеджер`
- `ЗаказПоставщику` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ЗаказПоставщику`
- `ПодотчетноеЛицо` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ПодотчетноеЛицо`
- `ВалютаВзаиморасчетов` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ВалютаВзаиморасчетов`
- `БанковскийСчетОрганизации` → `StandardODATA.Document_ПриобретениеТоваровУслуг_БанковскийСчетОрганизации`
- `Грузоотправитель` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Грузоотправитель`
- `БанковскийСчетКонтрагента` → `StandardODATA.Document_ПриобретениеТоваровУслуг_БанковскийСчетКонтрагента`
- `БанковскийСчетГрузоотправителя` → `StandardODATA.Document_ПриобретениеТоваровУслуг_БанковскийСчетГрузоотправителя`
- `Сделка` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Сделка`
- `Принял` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Принял`
- `ГруппаФинансовогоУчета` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ГруппаФинансовогоУчета`
- `Договор` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Договор`
- `Автор` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Автор`
- `Руководитель` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Руководитель`
- `ГлавныйБухгалтер` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ГлавныйБухгалтер`
- `СтатьяДвиженияДенежныхСредств` → `StandardODATA.Document_ПриобретениеТоваровУслуг_СтатьяДвиженияДенежныхСредств`
- `ПеревозчикПартнер` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ПеревозчикПартнер`
- `ЗонаДоставки` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ЗонаДоставки`
- `НаправлениеДеятельности` → `StandardODATA.Document_ПриобретениеТоваровУслуг_НаправлениеДеятельности`
- `Соглашение` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Соглашение`
- `Организация` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Организация`
- `АвансовыйОтчет` → `StandardODATA.Document_ПриобретениеТоваровУслуг_АвансовыйОтчет`
- `ОбъектРасчетовУпр` → `StandardODATA.Document_ПриобретениеТоваровУслуг_ОбъектРасчетовУпр`

## `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары`

Полей: 42

| Поле | Тип | Nullable |
|---|---|---|
| `Ref_Key` | `Edm.Guid` | false |
| `LineNumber` | `Edm.Int64` | false |
| `Номенклатура_Key` | `Edm.Guid` | true |
| `НоменклатураПартнера_Key` | `Edm.Guid` | true |
| `Характеристика_Key` | `Edm.Guid` | true |
| `Упаковка_Key` | `Edm.Guid` | true |
| `КоличествоУпаковок` | `Edm.Double` | true |
| `Количество` | `Edm.Double` | true |
| `КоличествоПоРНПТ` | `Edm.Double` | true |
| `Цена` | `Edm.Double` | true |
| `ВидЦеныПоставщика_Key` | `Edm.Guid` | true |
| `ПроцентРучнойСкидки` | `Edm.Double` | true |
| `СуммаРучнойСкидки` | `Edm.Double` | true |
| `Сумма` | `Edm.Double` | true |
| `СтавкаНДС_Key` | `Edm.Guid` | true |
| `СуммаНДС` | `Edm.Double` | true |
| `СуммаСНДС` | `Edm.Double` | true |
| `СтатьяРасходов_Key` | `Edm.Guid` | true |
| `АналитикаРасходов` | `Edm.String` | true |
| `КодСтроки` | `Edm.Int64` | true |
| `НомерГТД_Key` | `Edm.Guid` | true |
| `Склад_Key` | `Edm.Guid` | true |
| `ЗаказПоставщику_Key` | `Edm.Guid` | true |
| `Сертификат` | `Edm.String` | true |
| `НомерПаспорта` | `Edm.String` | true |
| `СтатусУказанияСерий` | `Edm.Int16` | true |
| `Сделка_Key` | `Edm.Guid` | true |
| `СуммаВзаиморасчетов` | `Edm.Double` | true |
| `СуммаНДСВзаиморасчетов` | `Edm.Double` | true |
| `ВидЗапасов_Key` | `Edm.Guid` | true |
| `ИдентификаторСтроки` | `Edm.String` | true |
| `Назначение_Key` | `Edm.Guid` | true |
| `Серия_Key` | `Edm.Guid` | true |
| `АналитикаУчетаНоменклатуры_Key` | `Edm.Guid` | true |
| `Подразделение_Key` | `Edm.Guid` | true |
| `СписатьНаРасходы` | `Edm.Boolean` | true |
| `НомерВходящегоДокумента` | `Edm.String` | true |
| `ДатаВходящегоДокумента` | `Edm.DateTime` | true |
| `ОбъектРасчетов_Key` | `Edm.Guid` | true |
| `НаименованиеВходящегоДокумента` | `Edm.String` | true |
| `СуммаИтог` | `Edm.Double` | true |
| `АналитикаРасходов_Type` | `Edm.String` | true |

**Navigation** (14):
- `Номенклатура` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_Номенклатура`
- `НоменклатураПартнера` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_НоменклатураПартнера`
- `Упаковка` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_Упаковка`
- `ВидЦеныПоставщика` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_ВидЦеныПоставщика`
- `СтавкаНДС` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_СтавкаНДС`
- `СтатьяРасходов` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_СтатьяРасходов`
- `Склад` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_Склад`
- `ЗаказПоставщику` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_ЗаказПоставщику`
- `Сделка` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_Сделка`
- `ВидЗапасов` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_ВидЗапасов`
- `Серия` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_Серия`
- `АналитикаУчетаНоменклатуры` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_АналитикаУчетаНоменклатуры`
- `Подразделение` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_Подразделение`
- `ОбъектРасчетов` → `StandardODATA.Document_ПриобретениеТоваровУслуг_Товары_ОбъектРасчетов`
