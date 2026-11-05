"""Ejecutar python -m unittest discover -s . -p test_etl_oc_api.py"""
import copy
import unittest
from datetime import date
from decimal import Decimal, localcontext

from etl_oc_api import NoElegible, transformar


class TransformacionTest(unittest.TestCase):
    def setUp(self):
        self.dims = {'producto':{'100','200'}, 'proveedor':{1},'comprador':{2},'tipo':{8}}
        self.oc = {
            'Codigo':'OC-TEST','CodigoLicitacion':'LIC-TEST',
            'Fechas':{'FechaEnvio':'2026-11-01T12:00:00'},'TipoMoneda':'CLP',
            'Proveedor':{'Codigo':'1'}, 'Comprador':{'CodigoOrganismo':'2'},'CodigoTipo':8,
            'TotalNeto':Decimal('30'),'Impuestos':Decimal('5.7'),
            'Cargos':Decimal('2'),'Descuentos':Decimal('1'),'Total':Decimal('36.7'),
            'Items':{'Cantidad':2,'Listado':[
                {'Correlativo':1,'CodigoProducto':100,'Moneda':'CLP','Cantidad':1,'PrecioNeto':10,
                 'TotalCargos':0,'TotalDescuentos':0,'TotalImpuestos':0,'Total':10},
                {'Correlativo':2,'CodigoProducto':200,'Moneda':'CLP','Cantidad':2,'PrecioNeto':10,
                 'TotalCargos':0,'TotalDescuentos':0,'TotalImpuestos':0,'Total':20},
            ]}}

    def transformar(self, oc=None, tasas=None):
        return transformar(oc or self.oc, 2026, tasas or {}, self.dims)

    def test_conciliacion_distribucion_y_entrada_intacta(self):
        antes = copy.deepcopy(self.oc)
        filas = self.transformar()
        with localcontext() as ctx:
            ctx.prec=60
            self.assertEqual(sum(r['monto_bruto_asignado_clp'] for r in filas),Decimal('36.7'))
            self.assertEqual(sum(r['impuesto_asignado_clp'] for r in filas),Decimal('5.7'))
            self.assertEqual(sum(r['cargo_oc_asignado_clp'] for r in filas),Decimal(2))
        self.assertEqual(self.oc,antes)
        self.assertEqual(filas[0]['id_item'],'api:1')

    def test_cantidad_fraccionaria(self):
        self.oc['Items']['Listado'][0].update(Cantidad=Decimal('0.5'),PrecioNeto=20)
        self.assertEqual(self.transformar()[0]['cantidad'],Decimal('0.5'))

    def test_no_carga_oc_parcial(self):
        self.oc['Items']['Cantidad']=3
        with self.assertRaisesRegex(NoElegible,'detalle_incompleto'):
            self.transformar()

    def test_sin_licitacion(self):
        self.oc.pop('CodigoLicitacion')
        with self.assertRaisesRegex(NoElegible,'sin_licitacion'):
            self.transformar()

    def test_dimensiones(self):
        self.oc['Items']['Listado'][1]['CodigoProducto']=999
        with self.assertRaisesRegex(NoElegible,'dimension_faltante'):
            self.transformar()

    def test_repetidos(self):
        self.oc['Items']['Listado'][1]['Correlativo']=1
        with self.assertRaisesRegex(NoElegible,'correlativo_duplicado'):
            self.transformar()

    def test_moneda_distinta(self):
        self.oc['Items']['Listado'][0]['Moneda']='USD'
        with self.assertRaisesRegex(NoElegible,'monedas_distintas'):
            self.transformar()

    def test_cotizacion(self):
        self.oc['TipoMoneda']='USD'
        for i in self.oc['Items']['Listado']:
            i['Moneda']='USD'
        with self.assertRaisesRegex(NoElegible,'paridad_faltante'):
            self.transformar()
        filas=self.transformar(tasas={(date(2026,11,1),'USD'):(Decimal(900),date(2026,10,30))})
        with localcontext() as ctx:
            ctx.prec=60
            self.assertEqual(sum(r['monto_bruto_asignado_clp'] for r in filas),Decimal('33030'))

    def test_cabecera_no_concilia(self):
        self.oc['Total']=Decimal(40)
        with self.assertRaisesRegex(NoElegible,'cabecera_no_concilia'):
            self.transformar()

    def test_no_inventa_ajustes_ausentes(self):
        self.oc['Items']['Listado'][0].pop('TotalImpuestos')
        with self.assertRaisesRegex(NoElegible,'numero ausente'):
            self.transformar()

    def test_total_item_no_concilia(self):
        self.oc['Items']['Listado'][1]['Total']=19
        with self.assertRaisesRegex(NoElegible,'total_item_no_concilia'):
            self.transformar()

    def test_anio(self):
        self.oc['Fechas']['FechaEnvio']='2025-11-01'
        with self.assertRaisesRegex(NoElegible,'fuera_del_anio'):
            self.transformar()


if __name__ == '__main__':
    unittest.main()
