import math         # Biblioteca de matematica
# from math import trunc  - importa somente a opção trunc
import random
import time


# Exercicio 04
# numero1 = int(input('Digite o primeiro número = '))
# numero2 = int(input('Digite o segundo número = '))
# soma = numero1 + numero2
# print('A somatória dos números {} e {}, vale {}'.format(numero1, numero2, soma))


# Exercicio 05
# n1 = int(input('Digite um número :'))
# antecessor = n1-1
# sucessor = n1+1
# print('O número é {}, o antecessor é {} e o sucessor é {}'.format(n1, antecessor, sucessor))

# Exercicio 6
# n2 = int(input('Digite um número :'))
# dobro = n2*2
# triplo = n2*3
# raiz = n2**2
# print('O número é {}, seu dobro é {} e o seu triplo é {}'.format(n2, dobro, triplo))


# Exercicio 7 - Calculo média de notas
# nota1 = int(input('Digite a primeira nota :'))
# nota2 = int(input('Digite a segunda nota :'))
# media = (nota1+nota2)/2
# print('A nota 1 é de: {}\nA nota 2 é de: {}\nE sua média é de: {} '.format(nota1, nota2, media))

# Exercicio 8 - Conversor de Metros

# metro = float(input('Digite a quantidade em Metros: '))
# centimetros = metro*100
# milimetros = metro*1000
# print('O valor em metros é de: {}\nO valor em centimetros é de: {}\nE o valor em milimetros é de: {}'.format(metro, centimetros, milimetros))

# Exercicio 9 - Tabuada de um número

# num = int(input('Digite um número: '))
# tabuada = num*1,num*2,num*3,num*4,num*5,num*6,num*7,num*8,num*9,num*10
# print('A tabuada do número é de: {}'.format(tabuada))

# Exercicio 10 - Conversor de moedas

# dinheiro = int(input('Quantos dinheiro você tem? '))
# conversao = dinheiro/3.27
# print('Você pode comprar {:.2f} dolares.'.format(conversao))



# Exercicio 11 - Pintando uma parede

# L = float(input('Qual a largura da parede? '))
# A = float(input('Qual a altura da parede? '))
# area = L * A
# print('Você possui uma parede de dimensão {}x{} e sua área é de {}m²'.format(L,A,area))
# tinta = area / 2
# print('Para pintar toda a parede, você precisará de {:.2f} litros de tinta'.format(tinta))



# Exercicio 12 - Calculando Desconto

# preco = float(input('Qual o preço do produto? '))
# desconto = preco*0.05
# novopreco = preco-desconto
# print('O preço do produto com desconto de 5% é de: {} '.format(novopreco))

# Exercicio 13 - Reajuste Salarial

# salario = float(input('Digite seu salário? '))
# acrescimo = salario*0.15
# novosalario = salario+acrescimo
# print('Seu novo salario com 15% de aumento, é de: {}'.format(novosalario))


# Exercicio 14 - Conversão temperatura

# c = float(input('Informe a temperatura em °C : '))
# f = ((9*c)/5)+32
# print('A temperatura em °F é de: {}'.format(f))

# Exercicio 15 - Aluguel de Carros
# dias = int(input('Quantos dias o carro ficou alugado? '))
# km = float(input('Quantos km rodados? '))
# pago = (dias * 60) + (km * 0.15)
# print('O total a pagar é de R${:.2f}'.format(pago))

# Exercicio 16 - Quebrando um número
# num = float(input('Digite um número: '))
# print('O valor digitado foi {} e a porção inteira é de: {}'.format(num, math.trunc(num)))

# Exercicio 17 - Calculo hipotenusa
# cat_oposto = float(input('Comprimento do cateto oposto: '))
# cat_adjacente = float(input('Comprimento do cateto adjacente: '))
# hipotenusa = (cat_oposto**2 + cat_adjacente**2)**(1/2)
# print('A hipotenusa vai medir {:.2f}'.format(hipotenusa))

# ou

# co = float(input('Comprimento do cateto oposto: '))
# ca = float(input('Comprimento do cateto adjacente: '))
# hi = math.hypot(co,ca)
# print('A hipotenusa vai medir {:.2f}'.format(hi))

# Exercicio 18 - Calculo Seno, Cosseno e Tangente
# an = float(input('Digite o ângulo que você deseja: '))
# sen = math.sin(math.radians(an))
# cos = math.cos(math.radians(an))
# tag = math.tan(math.radians(an))
# print('O ângulo de {:.1f} tem o Seno de {:.2f}\nO ângulo de {:.1f} tem o Cosseno de {:.2f}\nO ângulo de {:.1f} tem a Tangente de {:.2f}'.format(an,sen,an,cos,an,tag))


# Exercicio 19 -Sorteando um nome aleatório
# n1 = str(input('Nome primeiro aluno: '))
# n2 = str(input('Nome segundo aluno: '))
# n3 = str(input('Nome terceiro aluno: '))
# n4 = str(input('Nome quarto aluno: '))
# lista = [n1,n2,n3,n4]
# escolhido = random.choice(lista)
# print('O escolhido foi : {}'.format(escolhido))

# Exercicio 20 - Sorteando uma ordem de lista
# n1 = str(input('Nome primeiro aluno: '))
# n2 = str(input('Nome segundo aluno: '))
# n3 = str(input('Nome terceiro aluno: '))
# n4 = str(input('Nome quarto aluno: '))
# lista = [n1,n2,n3,n4]
# random.shuffle(lista)
# print('A ordem de apresentação será: ')
# print(lista)

# Exercicio 21 - Executando um mp3
# import pygame
# pygame.init()
# pygame.mixer.music.load('nome do arquivo mp3')
# pygame.mixer.music.play()
# pygame.event.wait()

# Exercicio 22 - Analisando um Nome Completo
# n = str(input('Digite seu nome completo: ')).strip()
# print('Analisando seu nome...')
# print('Seu nome em letra maiúscula é: {}'.format(n.upper()))
# print('Seu nome em letra minúscula é: {}'.format(n.lower()))
# print('Seu nome tem o total de letras {}'.format(len(n) - n.count(' ')))
# print('Seu primeiro nome tem {} letras'.format(n.find(' ')))

# Exercicio 23 - Separador de número

# num = int(input('Digite um número: '))
# u = num // 1 % 10
# d = num // 10 % 10
# c = num // 100 % 10
# m = num // 1000 % 10

# print('O número digitado foi: {}'.format(num))
# print('Unidade: {}'.format(u))
# print('Dezena {}'.format(d))
# print('Centena {}'.format(c))
# print('Milhar {}'.format(m))

# Exercicio 24 - Verificando as primeiras letras de um texto

# cidade = str(input('Em qual cidade você nasceu? ')).strip()
# print(cidade[0:5].upper() == 'SANTO')

# Exercicios 25 - Procurando uma String dentro de outra

# nome = str(input('Digite seu nome completo? ')).strip()
# print('Seu nome tem Silva ? {}'.format('SILVA' in nome).upper())

# Exercicio 26 - Primeira e ultima ocorrencia dentro de uma string

# frase = str(input('Digite uma frase: ')).strip().upper()
# print('A letra A aparece {} vezes: '.format(frase.count('A')))
# print('A primeira letra A aparece na posição {}'.format(frase.find('A')+1))
# print(('A última letra A aparece na posição {}'.format(frase.rfind('A')+1)))

# Exercicio 27 - Primeiro e último nome de uma pessoa

# nome = str(input('Digite seu nome completo: ')).strip()
# n = nome.split()
# print('Muito prazer em te conhecer! ')
# print('Seu primeiro nome é: {}'.format(n[0]))
# print('Seu último nome é : {}'.format(n[len(n)-1]))

# Exercicio 28 - Jogo de Adivinhação

# print('-=-' * 20)
# print('Vou pensar em um número de 0 a 5!!! Tente adivinhar...')
# print('-=-' * 20 )
# numero = int(input('Em que número em pensei:'))
# print('PROCESSANDO....')
# time.sleep(3)
# computador = random.randint(0,5)
# if numero == computador:
#    print('Parabéns, você conseguiu me vencer !!!')
# else:
#    print('Ganhei!! Eu pensei no número {} e não no número {}'.format(computador,numero))


# Exercicio 29 - Radar Eletronico



















