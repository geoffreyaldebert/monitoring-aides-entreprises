import dash
import dash_auth
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_table

import pandas as pd
import random
import json
import numpy as np

from datetime import datetime

import plotly.express as px

from secrets import *

from os import listdir

from isoweek import Week


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, 'https://codepen.io/chriddyp/pen/bWLwgP.css'],suppress_callback_exceptions=True)

df = pd.read_csv("./utils/departement2020.csv",dtype=str)
deps = df[['dep','reg','libelle']]
deps = deps.drop_duplicates(keep="first")
df = pd.read_csv("./utils/region2019.csv",dtype=str)
regs = df[['reg','libelle']]
regs = regs.drop_duplicates(keep="first")
deps = pd.merge(deps,regs,on="reg",how="left")


nafs = pd.read_csv("./utils/naf_complet_color.csv",dtype=str)
nafs = nafs[['code_section','libelle_section']]
nafs = nafs.drop_duplicates(keep="first")


# FDS
df = pd.read_csv("extract-stats-detail.csv",dtype=str)
df['week'] = df['date_paiement'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d').isocalendar()[1])
df.montant = df.montant.astype(float)
df.nombre = df.nombre.astype(float)
df['monday_date'] = df['week'].apply(lambda x: datetime.strftime(Week(2020, x).monday(),'%Y-%m-%d'))


# PGE
pge = pd.read_csv('pge-data.csv',dtype=str)
pge.montant = pge.montant.astype(float)

# Report
report = pd.read_csv('report-data.csv',dtype=str)
report.montant = report.montant.astype(float)

# CPSTI
cpsti = pd.read_csv('cpsti-data.csv',dtype=str)
cpsti.montant = cpsti.montant.astype(float)


def serve_layout():
    return html.Div([
            html.Div([
                html.H1("Evolution de la distribution des aides COVID-19")
            ]),
            html.Br(),
            dcc.Dropdown(
                id='geo-dropdown',
                options=[{"label":"national","value":"national"},{"label":"regional","value":"regional"},{"label":"departemental","value":"departemental"}],
                value="national"
            ),
            html.Br(),
            dcc.Dropdown(
                id='geo-level-dropdown',
                options=[]
            ),
            html.Br(),
            html.Div(id="graph-div-fds",children=[])
        ],style={'width':'1200px','margin':'auto'})


app.layout = serve_layout



@app.callback(
    dash.dependencies.Output("geo-level-dropdown", "options"),
    [dash.dependencies.Input("geo-dropdown", "value")],
)
def update_options(value):
    if(value == 'national'):
        return []
    elif(value == 'regional'):
        return [{'label':deps[deps['reg'] == o].iloc[0]['libelle_y'],'value':o,'value':o} for o in deps.reg.unique()]
    elif(value == 'departemental'):
        return [{'label':deps[deps['dep'] == o].iloc[0]['libelle_x'],'value':o,'value':o} for o in deps.dep.unique()]
    else:
        return []


@app.callback(
    dash.dependencies.Output("graph-div-fds", "children"),
    [dash.dependencies.Input("geo-level-dropdown", "value"),
    dash.dependencies.Input("geo-dropdown", "value")],
)
def update_graph_fds(geocode, geolevel):
    if(geolevel == 'national'):
        dfgeo = df[['monday_date','code_section','montant']].groupby(['monday_date','code_section'],as_index=False).sum()
        df3 = dfgeo.groupby(['code_section']).sum()
        df3 = df3.sort_values(by='montant',ascending=False)
        listcs = df3[:5].index
        df3 = dfgeo[~dfgeo.code_section.isin(listcs)][['monday_date','montant']].groupby(['monday_date'],as_index=False).sum()
        df3['code_section'] = 'Z'
        df4 = pd.concat([df3,dfgeo[dfgeo.code_section.isin(listcs)]])
        df4 = pd.merge(df4,nafs,on='code_section',how='left')
        df4.loc[df4.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
        fig = px.bar(df4, x="monday_date", y="montant", color="libelle_section", title="Fonds de solidarité - Montants versés au cours du temps")

        dfgeo = pge[['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
        df3 = dfgeo.groupby(['section_naf']).sum()
        df3 = df3.sort_values(by='montant',ascending=False)
        listcs = df3[:5].index
        df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
        df3['section_naf'] = 'Z'
        df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
        pgefinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
        for section in df4.section_naf.unique():
            df5 = df4[df4['section_naf'] == section]
            df5 = df5.sort_values(by='date')
            df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
            pgefinal = pd.concat([pgefinal,df5])
        pgefinal = pgefinal.rename(columns={'section_naf':'code_section'})
        pgefinal = pd.merge(pgefinal,nafs,on='code_section',how='left')
        pgefinal.loc[pgefinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
        fig2 = px.bar(pgefinal, x="date", y="delta_montant", color="libelle_section", title="Prêts garantis de l'Etat - Montants prêtés au cours du temps")


        dfgeo = report[['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
        df3 = dfgeo.groupby(['section_naf']).sum()
        df3 = df3.sort_values(by='montant',ascending=False)
        listcs = df3[:5].index
        df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
        df3['section_naf'] = 'Z'
        df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
        reportfinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
        for section in df4.section_naf.unique():
            df5 = df4[df4['section_naf'] == section]
            df5 = df5.sort_values(by='date')
            df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
            reportfinal = pd.concat([reportfinal,df5])
        reportfinal = reportfinal.rename(columns={'section_naf':'code_section'})
        reportfinal = pd.merge(reportfinal,nafs,on='code_section',how='left')
        reportfinal.loc[reportfinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
        fig3 = px.bar(reportfinal, x="date", y="delta_montant", color="libelle_section", title="Reports d'échéances fiscales - Montants reportés au cours du temps")



        dfgeo = cpsti[['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
        df3 = dfgeo.groupby(['section_naf']).sum()
        df3 = df3.sort_values(by='montant',ascending=False)
        listcs = df3[:5].index
        df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
        df3['section_naf'] = 'Z'
        df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
        cpstifinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
        for section in df4.section_naf.unique():
            df5 = df4[df4['section_naf'] == section]
            df5 = df5.sort_values(by='date')
            df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
            cpstifinal = pd.concat([cpstifinal,df5])
        cpstifinal = cpstifinal.rename(columns={'section_naf':'code_section'})
        cpstifinal = pd.merge(cpstifinal,nafs,on='code_section',how='left')
        cpstifinal.loc[cpstifinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
        fig4 = px.bar(cpstifinal, x="date", y="delta_montant", color="libelle_section", title="Aides exceptionnelles artisans / commerçants - Montants versés au cours du temps")


        return html.Div(id="content-div",children=[
            html.Div(children=[
                dcc.Graph(figure=fig),
            ]),           
            html.Div(children=[
                dcc.Graph(figure=fig2),
            ]),          
            html.Div(children=[
                dcc.Graph(figure=fig3),
            ]),
            html.Div(children=[
                dcc.Graph(figure=fig4),
            ]),

        ])
    if(geolevel == 'regional'):
        if((geocode != None) & (geocode in deps.reg.unique())):
            dfgeo = df[df['reg'] == geocode][['monday_date','code_section','montant']].groupby(['monday_date','code_section'],as_index=False).sum()
            df3 = dfgeo.groupby(['code_section']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.code_section.isin(listcs)][['monday_date','montant']].groupby(['monday_date'],as_index=False).sum()
            df3['code_section'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.code_section.isin(listcs)]])
            df4 = pd.merge(df4,nafs,on='code_section',how='left')
            df4.loc[df4.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig = px.bar(df4, x="monday_date", y="montant", color="libelle_section", title="Fonds de solidarité - Montants versés au cours du temps")


            dfgeo = pge[pge['reg'] == geocode][['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
            df3 = dfgeo.groupby(['section_naf']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
            df3['section_naf'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
            pgefinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
            for section in df4.section_naf.unique():
                df5 = df4[df4['section_naf'] == section]
                df5 = df5.sort_values(by='date')
                df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
                pgefinal = pd.concat([pgefinal,df5])
            pgefinal = pgefinal.rename(columns={'section_naf':'code_section'})
            pgefinal = pd.merge(pgefinal,nafs,on='code_section',how='left')
            pgefinal.loc[pgefinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig2 = px.bar(pgefinal, x="date", y="delta_montant", color="libelle_section", title="Prêts garantis de l'Etat - Montants prêtés au cours du temps")

            dfgeo = report[report['reg'] == geocode][['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
            df3 = dfgeo.groupby(['section_naf']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
            df3['section_naf'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
            reportfinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
            for section in df4.section_naf.unique():
                df5 = df4[df4['section_naf'] == section]
                df5 = df5.sort_values(by='date')
                df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
                reportfinal = pd.concat([reportfinal,df5])
            reportfinal = reportfinal.rename(columns={'section_naf':'code_section'})
            reportfinal = pd.merge(reportfinal,nafs,on='code_section',how='left')
            reportfinal.loc[reportfinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig3 = px.bar(reportfinal, x="date", y="delta_montant", color="libelle_section", title="Reports d'échéances fiscales - Montants reportés au cours du temps")

            dfgeo = cpsti[cpsti['reg'] == geocode][['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
            df3 = dfgeo.groupby(['section_naf']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
            df3['section_naf'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
            cpstifinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
            for section in df4.section_naf.unique():
                df5 = df4[df4['section_naf'] == section]
                df5 = df5.sort_values(by='date')
                df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
                cpstifinal = pd.concat([cpstifinal,df5])
            cpstifinal = cpstifinal.rename(columns={'section_naf':'code_section'})
            cpstifinal = pd.merge(cpstifinal,nafs,on='code_section',how='left')
            cpstifinal.loc[cpstifinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig4 = px.bar(cpstifinal, x="date", y="delta_montant", color="libelle_section", title="Aides exceptionnelles artisans / commerçants - Montants versés au cours du temps")


            return html.Div(id="content-div",children=[
                html.Div(children=[
                    dcc.Graph(figure=fig),
                ]),     
                html.Div(children=[
                    dcc.Graph(figure=fig2),
                ]),     
                html.Div(children=[
                    dcc.Graph(figure=fig3),
                ]),     
                html.Div(children=[
                    dcc.Graph(figure=fig4),
                ])
            ]) 
        else:
            return html.Div(id="content-div",children=[
                html.P("Sélectionner une région")
            ])
    if(geolevel == 'departemental'):
        if((geocode != None) & (geocode in deps.dep.unique())):
            dfgeo = df[df['dep'] == geocode][['monday_date','code_section','montant']].groupby(['monday_date','code_section'],as_index=False).sum()
            df3 = dfgeo.groupby(['code_section']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.code_section.isin(listcs)][['monday_date','montant']].groupby(['monday_date'],as_index=False).sum()
            df3['code_section'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.code_section.isin(listcs)]])
            df4 = pd.merge(df4,nafs,on='code_section',how='left')
            df4.loc[df4.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig = px.bar(df4, x="monday_date", y="montant", color="libelle_section", title="Fonds de solidarité - Montants versés au cours du temps")


            dfgeo = pge[pge['dep'] == geocode][['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
            df3 = dfgeo.groupby(['section_naf']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
            df3['section_naf'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
            pgefinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
            for section in df4.section_naf.unique():
                df5 = df4[df4['section_naf'] == section]
                df5 = df5.sort_values(by='date')
                df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
                pgefinal = pd.concat([pgefinal,df5])
            pgefinal = pgefinal.rename(columns={'section_naf':'code_section'})
            pgefinal = pd.merge(pgefinal,nafs,on='code_section',how='left')
            pgefinal.loc[pgefinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig2 = px.bar(pgefinal.dropna(), x="date", y="delta_montant", color="libelle_section", title="Prêts garantis de l'Etat - Montants prêtés au cours du temps")


            dfgeo = report[report['dep'] == geocode][['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
            df3 = dfgeo.groupby(['section_naf']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
            df3['section_naf'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
            reportfinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
            for section in df4.section_naf.unique():
                df5 = df4[df4['section_naf'] == section]
                df5 = df5.sort_values(by='date')
                df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
                reportfinal = pd.concat([reportfinal,df5])
            reportfinal = reportfinal.rename(columns={'section_naf':'code_section'})
            reportfinal = pd.merge(reportfinal,nafs,on='code_section',how='left')
            reportfinal.loc[reportfinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig3 = px.bar(reportfinal.dropna(), x="date", y="delta_montant", color="libelle_section", title="Reports d'échéances fiscales - Montants reportés au cours du temps")

            dfgeo = cpsti[cpsti['dep'] == geocode][['date','section_naf','montant']].groupby(['date','section_naf'],as_index=False).sum()
            df3 = dfgeo.groupby(['section_naf']).sum()
            df3 = df3.sort_values(by='montant',ascending=False)
            listcs = df3[:5].index
            df3 = dfgeo[~dfgeo.section_naf.isin(listcs)][['date','montant']].groupby(['date'],as_index=False).sum()
            df3['section_naf'] = 'Z'
            df4 = pd.concat([df3,dfgeo[dfgeo.section_naf.isin(listcs)]])
            cpstifinal = pd.DataFrame(columns=['date','montant', 'section_naf','delta_montant'])
            for section in df4.section_naf.unique():
                df5 = df4[df4['section_naf'] == section]
                df5 = df5.sort_values(by='date')
                df5['delta_montant'] = df5['montant'].diff().fillna(df5['montant'])
                cpstifinal = pd.concat([cpstifinal,df5])
            cpstifinal = cpstifinal.rename(columns={'section_naf':'code_section'})
            cpstifinal = pd.merge(cpstifinal,nafs,on='code_section',how='left')
            cpstifinal.loc[cpstifinal.code_section == 'Z', 'libelle_section'] = "Autres sections NAF"
            fig4 = px.bar(cpstifinal, x="date", y="delta_montant", color="libelle_section", title="Aides exceptionnelles artisans / commerçants - Montants versés au cours du temps")


            return html.Div(id="content-div",children=[
                html.Div(children=[
                    dcc.Graph(figure=fig),
                ]),     
                html.Div(children=[
                    dcc.Graph(figure=fig2),
                ]),     
                html.Div(children=[
                    dcc.Graph(figure=fig3),
                ]),     
                html.Div(children=[
                    dcc.Graph(figure=fig4),
                ])
            ])
        else:
            return html.Div(id="content-div",children=[
                html.P("Sélectionner un département")
            ])

    


if __name__ == '__main__':
    app.run_server(debug=True,port=7456)
